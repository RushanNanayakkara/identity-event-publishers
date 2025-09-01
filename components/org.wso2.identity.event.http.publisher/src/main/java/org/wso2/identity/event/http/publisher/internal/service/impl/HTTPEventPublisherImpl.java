/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.identity.event.http.publisher.internal.service.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.slf4j.MDC;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.identity.core.util.IdentityTenantUtil;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherException;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherServerException;
import org.wso2.carbon.identity.event.publisher.api.model.EventContext;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.carbon.identity.event.publisher.api.service.EventPublisher;
import org.wso2.carbon.identity.webhook.management.api.exception.WebhookMgtException;
import org.wso2.carbon.identity.webhook.management.api.model.Webhook;
import org.wso2.carbon.utils.DiagnosticLog;
import org.wso2.identity.event.http.publisher.api.exception.HTTPAdapterException;
import org.wso2.identity.event.http.publisher.internal.component.ClientManager;
import org.wso2.identity.event.http.publisher.internal.component.HTTPAdapterDataHolder;
import org.wso2.identity.event.http.publisher.internal.constant.HTTPAdapterConstants;
import org.wso2.identity.event.http.publisher.internal.util.HTTPAdapterUtil;
import org.wso2.identity.event.http.publisher.internal.util.HTTPCorrelationLogUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyMap;
import static org.wso2.carbon.CarbonConstants.LogEventConstants.TENANT_ID;
import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.CORRELATION_ID_MDC;
import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.TENANT_DOMAIN;
import static org.wso2.identity.event.http.publisher.internal.constant.ErrorMessage.ERROR_ACTIVE_WEBHOOKS_RETRIEVAL;
import static org.wso2.identity.event.http.publisher.internal.util.HTTPAdapterUtil.printPublisherDiagnosticLog;
import static org.wso2.identity.event.http.publisher.internal.util.HTTPCorrelationLogUtils.handleResponseCorrelationLog;

/**
 * OSGi service for publishing events using http adapter.
 */
public class HTTPEventPublisherImpl implements EventPublisher {

    private static final Log log = LogFactory.getLog(HTTPEventPublisherImpl.class);

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

    @Override
    public String getAssociatedAdapter() {

        return HTTPAdapterConstants.HTTP_ADAPTER_NAME;
    }

    @Override
    public void publish(SecurityEventTokenPayload eventPayload, EventContext eventContext)
            throws EventPublisherException {

        makeAsyncAPICall(eventPayload, eventContext);
    }

    @Override
    public boolean canHandleEvent(EventContext eventContext) throws EventPublisherException {

        try {
            final List<Webhook> activeWebhooks = HTTPAdapterDataHolder.getInstance().getWebhookManagementService()
                    .getActiveWebhooks(eventContext.getEventProfileName(), eventContext.getEventProfileVersion(),
                            eventContext.getEventUri(), eventContext.getTenantDomain());
            return !activeWebhooks.isEmpty();
        } catch (WebhookMgtException e) {
            throw new EventPublisherServerException(ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getMessage(),
                    ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getDescription(), ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getCode(), e);
        }
    }

    private void makeAsyncAPICall(SecurityEventTokenPayload eventPayload, EventContext eventContext)
            throws EventPublisherServerException {

        // Freeze immutable per-publish values; reuse across retries.
        final String correlationId = HTTPAdapterUtil.getCorrelationID(eventPayload);
        final String tenantDomain = eventContext.getTenantDomain();
        final int tenantId = IdentityTenantUtil.getTenantId(eventContext.getTenantDomain());

        final String eventProfileName = eventContext.getEventProfileName();
        final String eventProfileUri = eventContext.getEventUri();
        final String events = String.join(",", eventPayload.getEvents().keySet());

        final Map<String, String> copiedMDCSnapshot =
                MDC.getCopyOfContextMap() != null ? MDC.getCopyOfContextMap() : emptyMap();

        final String bodyJson;
        try {
            bodyJson = MAPPER.writeValueAsString(eventPayload);
        } catch (JsonProcessingException e) {
            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, null,
                    HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.FAILED,
                    "Failed to serialize HTTP adapter payload.");
            return;
        }

        final List<Webhook> activeWebhooks;
        try {
            activeWebhooks = HTTPAdapterDataHolder.getInstance().getWebhookManagementService()
                    .getActiveWebhooks(eventContext.getEventProfileName(), eventContext.getEventProfileVersion(),
                            eventContext.getEventUri(), eventContext.getTenantDomain());
        } catch (WebhookMgtException e) {
            throw new EventPublisherServerException(ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getMessage(),
                    ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getDescription(), ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getCode(), e);
        }

        for (Webhook webhook : activeWebhooks) {
            final String url = webhook.getEndpoint();
            final String secret = webhook.getSecret();

            sendWithRetries(eventProfileName, eventProfileUri, events,
                    bodyJson, copiedMDCSnapshot, correlationId, tenantDomain, tenantId, url, secret,
                    HTTPAdapterDataHolder.getInstance().getClientManager().getMaxRetries());
        }
    }

    /**
     * Retries always reuse the frozen snapshots; no shared mutable state is read here.
     */
    private void sendWithRetries(String eventProfileName, String eventProfileUri, String events, String bodyJson,
                                 Map<String, String> mdcSnapshot, String correlationId, String tenantDomain,
                                 int tenantId, String url, String secret, int retriesLeft) {

        ClientManager clientManager = HTTPAdapterDataHolder.getInstance().getClientManager();

        final HttpPost request;
        try {
            request = clientManager.createHttpPost(url, bodyJson, secret);
        } catch (HTTPAdapterException e) {
            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                    HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.FAILED,
                    "Failed to construct HTTP request for HTTP adapter publish.");
            log.debug("Error constructing HTTP request for HTTP adapter publish. No retries will be attempted.", e);
            return;
        }

        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.SUCCESS,
                "Publishing event data to endpoint.");

        final long requestStartTime = System.currentTimeMillis();

        CompletableFuture<HttpResponse> future = clientManager.executeAsync(request);

        future.whenCompleteAsync((response, throwable) -> {
            try {
                MDC.clear();
                if (mdcSnapshot != null && !mdcSnapshot.isEmpty()) {
                    MDC.setContextMap(mdcSnapshot);
                }
                if (StringUtils.isNotBlank(correlationId)) {
                    MDC.put(CORRELATION_ID_MDC, correlationId);
                }
                MDC.put(TENANT_DOMAIN, tenantDomain);
                MDC.put(TENANT_ID, String.valueOf(tenantId));
                PrivilegedCarbonContext.startTenantFlow();
                PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantId(tenantId);
                PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantDomain(tenantDomain);

                if (throwable == null) {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        handleResponseCorrelationLog(request, requestStartTime,
                                HTTPCorrelationLogUtils.RequestStatus.COMPLETED.getStatus(),
                                String.valueOf(status), response.getStatusLine().getReasonPhrase());
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.SUCCESS, "Event data published to endpoint.");
                        log.debug("HTTP request completed. Response code: " + status +
                                ", Endpoint: " + url + ", Event URI: " + eventProfileUri);
                    } else {
                        if (retriesLeft > 0) {
                            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                    HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                    DiagnosticLog.ResultStatus.FAILED,
                                    "Publish attempt failed with status code: " + status +
                                            ". Retrying… (" + retriesLeft + " attempts left)");
                            // Retry with the SAME snapshots
                            sendWithRetries(eventProfileName, eventProfileUri, events, bodyJson, mdcSnapshot,
                                    correlationId, tenantDomain, tenantId, url, secret, retriesLeft - 1);
                        } else {
                            handleResponseCorrelationLog(request, requestStartTime,
                                    HTTPCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                                    String.valueOf(status), response.getStatusLine().getReasonPhrase());
                            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                    HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                    DiagnosticLog.ResultStatus.FAILED,
                                    "Failed to publish event data to endpoint. Status code: " + status +
                                            ". Maximum retries reached.");
                            log.warn("Failed to publish event data to endpoint: " + url + ". Maximum retries reached.");
                        }
                    }
                } else {
                    if (retriesLeft > 0) {
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                "Publish attempt failed due to exception. Retrying… (" +
                                        retriesLeft + " attempts left)");
                        // Retry with the SAME snapshots
                        sendWithRetries(eventProfileName, eventProfileUri, events, bodyJson, mdcSnapshot, correlationId,
                                tenantDomain, tenantId, url, secret, retriesLeft - 1);
                    } else {
                        handleResponseCorrelationLog(request, requestStartTime,
                                HTTPCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                                throwable.getMessage());
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                "Failed to publish event data to endpoint. Maximum retries reached.");
                        log.warn("Failed to publish event data to endpoint: " + url + ". Maximum retries reached.");
                        log.debug("Failed to publish event data to endpoint: " + url, throwable);
                    }
                }
            } finally {
                if (response.getEntity() != null) {
                    EntityUtils.consumeQuietly(response.getEntity());
                }
                if (StringUtils.isNotEmpty(correlationId)) {
                    MDC.remove(CORRELATION_ID_MDC);
                }
                MDC.remove(TENANT_DOMAIN);
                MDC.remove(TENANT_ID);
                PrivilegedCarbonContext.endTenantFlow();
                MDC.clear();
            }
        }, clientManager.getAsyncCallbackExecutor());
    }
}
