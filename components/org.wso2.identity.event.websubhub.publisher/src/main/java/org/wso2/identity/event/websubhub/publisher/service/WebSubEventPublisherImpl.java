/*
 * Copyright (c) 2024-2025, WSO2 LLC. (http://www.wso2.com).
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

package org.wso2.identity.event.websubhub.publisher.service;

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
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherException;
import org.wso2.carbon.identity.event.publisher.api.model.EventContext;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.carbon.identity.event.publisher.api.service.EventPublisher;
import org.wso2.carbon.identity.topic.management.api.exception.TopicManagementException;
import org.wso2.carbon.utils.DiagnosticLog;
import org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterException;
import org.wso2.identity.event.websubhub.publisher.internal.ClientManager;
import org.wso2.identity.event.websubhub.publisher.internal.WebSubHubAdapterDataHolder;
import org.wso2.identity.event.websubhub.publisher.util.WebSubHubCorrelationLogUtils;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.CORRELATION_ID_MDC;
import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.TENANT_DOMAIN;
import static org.wso2.carbon.identity.event.publisher.api.constant.ErrorMessage.ERROR_CODE_CONSTRUCTING_HUB_TOPIC;
import static org.wso2.carbon.identity.event.publisher.api.constant.ErrorMessage.ERROR_CODE_TOPIC_EXISTS_CHECK;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.PUBLISH;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.buildURL;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.constructHubTopic;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.getCorrelationID;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.getWebSubBaseURL;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleResponseCorrelationLog;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleServerException;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.printPublisherDiagnosticLog;

/**
 * OSGi service for publishing events using web sub hub.
 */
public class WebSubEventPublisherImpl implements EventPublisher {

    private static final Log log = LogFactory.getLog(WebSubEventPublisherImpl.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

    @Override
    public String getAssociatedAdapter() {

        return WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME;
    }

    @Override
    public void publish(SecurityEventTokenPayload eventPayload, EventContext eventContext)
            throws EventPublisherException {

        try {
            // Build immutable per-publish values.
            final String topic = constructHubTopic(
                    eventContext.getEventUri(),
                    eventContext.getEventProfileName(),
                    eventContext.getEventProfileVersion(),
                    eventContext.getTenantDomain());

            final String url = buildURL(topic, getWebSubBaseURL(), PUBLISH);
            final String bodyJson = MAPPER.writeValueAsString(eventPayload);

            final String correlationId = getCorrelationID(eventPayload);
            final String tenantDomain = eventContext.getTenantDomain() + "-" + UUID.randomUUID();

            final String eventProfileName = eventContext.getEventProfileName();
            final String eventProfileUri = eventContext.getEventUri();
            final String events = String.join(",", eventPayload.getEvents().keySet());

            sendWithRetries(eventProfileName, eventProfileUri, events, bodyJson, correlationId, tenantDomain, url,
                    WebSubHubAdapterDataHolder.getInstance().getClientManager().getMaxRetries());
        } catch (WebSubAdapterException e) {
            throw handleServerException(ERROR_CODE_CONSTRUCTING_HUB_TOPIC, e,
                    WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME);
        } catch (JsonProcessingException e) {
            throw handleServerException(ERROR_CODE_CONSTRUCTING_HUB_TOPIC, e,
                    "Error serializing event payload");
        }
    }

    @Override
    public boolean canHandleEvent(EventContext eventContext) throws EventPublisherException {

        try {
            return WebSubHubAdapterDataHolder.getInstance().getTopicManagementService()
                    .isTopicExists(eventContext.getEventUri(), eventContext.getEventProfileName(),
                            eventContext.getEventProfileVersion(), eventContext.getTenantDomain());
        } catch (TopicManagementException e) {
            throw handleServerException(ERROR_CODE_TOPIC_EXISTS_CHECK, e,
                    WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME);
        }
    }

    private void sendWithRetries(String eventProfileName, String eventProfileUri, String events, String bodyJson,
                                 String correlationId, String tenantDomain, String url, int retriesLeft) {

        ClientManager clientManager = WebSubHubAdapterDataHolder.getInstance().getClientManager();

        final HttpPost request;
        try {
            request = clientManager.createHttpPost(url, bodyJson, correlationId);
        } catch (WebSubAdapterException e) {
            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                    WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.FAILED,
                    "Failed to construct HTTP request for WebSubHub publish.");
            log.warn("Failed to construct HTTP request for WebSubHub publish. No retries will be attempted.", e);
            return;
        }

        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                DiagnosticLog.ResultStatus.SUCCESS,
                "Publishing event data to WebSubHub.");
        log.debug("Event publishing to WebSubHub invoked.");

        final long requestStartTime = System.currentTimeMillis();

        CompletableFuture<HttpResponse> future = clientManager.executeAsync(request);
        future.whenCompleteAsync((response, throwable) -> {
            try {
                PrivilegedCarbonContext.startTenantFlow();
                PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantDomain(tenantDomain);
                if (StringUtils.isNotBlank(correlationId)) {
                    MDC.put(CORRELATION_ID_MDC, correlationId);
                }
                MDC.put(TENANT_DOMAIN, tenantDomain);

                if (throwable == null) {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        handleAsyncResponse(eventProfileName, eventProfileUri, events, response, request,
                                requestStartTime);
                    } else {
                        if (retriesLeft > 0) {
                            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                                    WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                    DiagnosticLog.ResultStatus.FAILED,
                                    "Publish attempt failed with status code: " + status +
                                            ". Retrying… (" + retriesLeft + " attempts left)");
                            log.debug("Publish attempt failed with status code: " + status +
                                    ". Retrying… (" + retriesLeft + " attempts left)");
                            sendWithRetries(eventProfileName, eventProfileUri, events, bodyJson, correlationId,
                                    tenantDomain, url, retriesLeft - 1);
                        } else {
                            handleResponseCorrelationLog(request, requestStartTime,
                                    WebSubHubCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                                    String.valueOf(status),
                                    response.getStatusLine().getReasonPhrase());
                            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                                    WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                    DiagnosticLog.ResultStatus.FAILED,
                                    "Failed to publish event data to WebSubHub. Status code: " + status +
                                            ". Maximum retries reached.");
                            log.error("Failed to publish event data to WebSubHub: " + url +
                                    ". Maximum retries reached.");
                            try {
                                if (response.getEntity() != null) {
                                    String body = EntityUtils.toString(response.getEntity());
                                    log.debug("Error response data: " + body);
                                } else {
                                    log.debug("WebSubHub event publisher received " + status +
                                            ". Response entity is null.");
                                }
                            } catch (IOException e) {
                                printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                                        WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                        DiagnosticLog.ResultStatus.FAILED,
                                        "Error while reading WebSubHub event publisher");
                                log.debug("Error while reading WebSubHub response.", e);
                            }
                        }
                    }
                } else {
                    if (retriesLeft > 0) {
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                "Publish attempt failed due to exception. Retrying… (" +
                                        retriesLeft + " attempts left)");
                        log.debug("Publish attempt failed due to exception. Retrying… (" +
                                retriesLeft + " attempts left)", throwable);
                        sendWithRetries(eventProfileName, eventProfileUri, events, bodyJson, correlationId,
                                tenantDomain, url, retriesLeft - 1);
                    } else {
                        handleResponseCorrelationLog(request, requestStartTime,
                                WebSubHubCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                                throwable.getMessage());
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                "Failed to publish event data to WebSubHub. Maximum retries reached.");
                        log.error("Failed to publish event data to WebSubHub: " + url +
                                ". Maximum retries reached.");
                    }
                }
            } finally {
                if (StringUtils.isNotBlank(correlationId)) {
                    MDC.remove(CORRELATION_ID_MDC);
                }
                MDC.remove(TENANT_DOMAIN);
                PrivilegedCarbonContext.endTenantFlow();
            }
        }, clientManager.getAsyncCallbackExecutor());
    }

    private static void handleAsyncResponse(String eventProfileName, String eventProfileUri, String events,
                                            HttpResponse response, HttpPost request, long requestStartTime) {

        int responseCode = response.getStatusLine().getStatusCode();
        String responsePhrase = response.getStatusLine().getReasonPhrase();

        log.debug("WebSubHub request completed. Response code: " + responseCode);

        handleResponseCorrelationLog(request, requestStartTime,
                WebSubHubCorrelationLogUtils.RequestStatus.COMPLETED.getStatus(),
                String.valueOf(responseCode), responsePhrase);

        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                DiagnosticLog.ResultStatus.SUCCESS,
                "Event data published to WebSubHub. Status code: " + responseCode);
        try {
            if (response.getEntity() != null) {
                log.debug("Response data: " + EntityUtils.toString(response.getEntity()));
            } else {
                log.debug("Response entity is null.");
            }
        } catch (IOException e) {
            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                    WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                    DiagnosticLog.ResultStatus.FAILED,
                    "Error while reading WebSubHub event publisher response.");
            log.debug("Error while reading WebSubHub event publisher response.", e);
        }
    }
}
