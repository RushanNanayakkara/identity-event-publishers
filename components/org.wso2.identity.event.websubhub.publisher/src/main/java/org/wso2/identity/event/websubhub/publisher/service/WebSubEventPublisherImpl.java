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
import java.util.concurrent.CompletableFuture;

import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.CORRELATION_ID_MDC;
import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.TENANT_DOMAIN;
import static org.wso2.carbon.identity.event.publisher.api.constant.ErrorMessage.ERROR_CODE_CONSTRUCTING_HUB_TOPIC;
import static org.wso2.carbon.identity.event.publisher.api.constant.ErrorMessage.ERROR_CODE_TOPIC_EXISTS_CHECK;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.CORRELATION_ID_REQUEST_HEADER;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.PUBLISH;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.buildURL;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.constructHubTopic;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.getWebSubBaseURL;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleResponseCorrelationLog;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleServerException;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.printPublisherDiagnosticLog;

/**
 * OSGi service for publishing events using web sub hub.
 */
public class WebSubEventPublisherImpl implements EventPublisher {

    private static final Log log = LogFactory.getLog(WebSubEventPublisherImpl.class);

    @Override
    public String getAssociatedAdapter() {

        return WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME;
    }

    @Override
    public void publish(SecurityEventTokenPayload eventPayload, EventContext eventContext)
            throws EventPublisherException {

        try {
            makeAsyncAPICall(eventPayload, eventContext,
                    constructHubTopic(eventContext.getEventUri(), eventContext.getEventProfileName(),
                            eventContext.getEventProfileVersion(), eventContext.getTenantDomain()),
                    getWebSubBaseURL());
            log.debug("Event publishing to WebSubHub invoked.");
        } catch (WebSubAdapterException e) {
            throw handleServerException(ERROR_CODE_CONSTRUCTING_HUB_TOPIC, e,
                    WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME);
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

    private void makeAsyncAPICall(SecurityEventTokenPayload eventPayload, EventContext eventContext,
                                  String topic, String webSubHubBaseUrl) throws WebSubAdapterException {

        String url = buildURL(topic, webSubHubBaseUrl, PUBLISH);
        printPublisherDiagnosticLog(eventContext, eventPayload,
                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.SUCCESS,
                "Publishing event data to WebSubHub.");

        sendWithRetries(eventPayload, eventContext, url,
                WebSubHubAdapterDataHolder.getInstance().getClientManager().getMaxRetries());
    }

    private void sendWithRetries(SecurityEventTokenPayload eventPayload, EventContext eventContext, String url,
                                 int retriesLeft) {

        ClientManager clientManager = WebSubHubAdapterDataHolder.getInstance().getClientManager();
        final HttpPost request;
        try {
            request = clientManager.createHttpPost(url, eventPayload);
        } catch (WebSubAdapterException e) {
            printPublisherDiagnosticLog(eventContext, eventPayload,
                    WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.FAILED,
                    "Failed to construct HTTP request for WebSubHub publish.");
            log.debug("Error constructing HTTP request for WebSubHub publish. No retries will be attempted.", e);
            return;
        }

        final long requestStartTime = System.currentTimeMillis();
        final String correlationId = request.getFirstHeader(CORRELATION_ID_REQUEST_HEADER).getValue();

        CompletableFuture<HttpResponse> future = clientManager.executeAsync(request);

        future.whenCompleteAsync((response, throwable) -> {
            try {
                PrivilegedCarbonContext.startTenantFlow();
                PrivilegedCarbonContext.getThreadLocalCarbonContext()
                        .setTenantDomain(eventContext.getTenantDomain());
                MDC.put(CORRELATION_ID_MDC, correlationId);
                MDC.put(TENANT_DOMAIN, eventContext.getTenantDomain());
                if (throwable == null) {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        handleAsyncResponse(response, eventPayload, request, requestStartTime, eventContext);
                    } else {
                        if (retriesLeft > 0) {
                            printPublisherDiagnosticLog(eventContext, eventPayload,
                                    WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                    DiagnosticLog.ResultStatus.FAILED,
                                    "Publish attempt failed with status code: " + status +
                                            ". Retrying… (" + retriesLeft + " attempts left)");
                            sendWithRetries(eventPayload, eventContext, url, retriesLeft - 1);
                        } else {
                            handleResponseCorrelationLog(request, requestStartTime,
                                    WebSubHubCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                                    String.valueOf(status),
                                    response.getStatusLine().getReasonPhrase());
                            printPublisherDiagnosticLog(eventContext, eventPayload,
                                    WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                    DiagnosticLog.ResultStatus.FAILED,
                                    "Failed to publish event data to WebSubHub. Status code: " + status +
                                            ". Maximum retries reached.");
                            log.error(
                                    "Failed to publish event data to websubhub: " + url + ". Maximum retries reached.");
                            try {
                                if (response.getEntity() != null) {
                                    String body = EntityUtils.toString(response.getEntity());
                                    log.debug("Error response data: " + body);
                                } else {
                                    log.debug("WebSubHub event publisher received " + status +
                                            ". Response entity is null.");
                                }
                            } catch (IOException e) {
                                printPublisherDiagnosticLog(eventContext, eventPayload,
                                        WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                        DiagnosticLog.ResultStatus.FAILED,
                                        "Error while reading WebSubHub event publisher");
                                log.debug("Error while reading WebSubHub response.", e);
                            }
                        }
                    }
                } else {
                    if (retriesLeft > 0) {
                        printPublisherDiagnosticLog(eventContext, eventPayload,
                                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                "Publish attempt failed due to exception. Retrying… (" +
                                        retriesLeft + " attempts left)");
                        sendWithRetries(eventPayload, eventContext, url, retriesLeft - 1);
                    } else {
                        handleResponseCorrelationLog(request, requestStartTime,
                                WebSubHubCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                                throwable.getMessage());
                        printPublisherDiagnosticLog(eventContext, eventPayload,
                                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                "Failed to publish event data to WebSubHub. Maximum retries reached.");
                        log.error("Failed to publish event data to websubhub: " + url + ". Maximum retries reached.");
                    }
                }
            } finally {
                MDC.remove(CORRELATION_ID_MDC);
                MDC.remove(TENANT_DOMAIN);
                PrivilegedCarbonContext.endTenantFlow();
            }
        }, clientManager.getAsyncCallbackExecutor());
    }

    private static void handleAsyncResponse(HttpResponse response, SecurityEventTokenPayload eventPayload,
                                            HttpPost request,
                                            long requestStartTime,
                                            EventContext eventContext) {

        int responseCode = response.getStatusLine().getStatusCode();
        String responsePhrase = response.getStatusLine().getReasonPhrase();
        log.debug("WebSubHub request completed. Response code: " + responseCode);

        handleResponseCorrelationLog(request, requestStartTime,
                WebSubHubCorrelationLogUtils.RequestStatus.COMPLETED.getStatus(),
                String.valueOf(responseCode), responsePhrase);

        printPublisherDiagnosticLog(eventContext, eventPayload,
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
            printPublisherDiagnosticLog(eventContext, eventPayload,
                    WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                    DiagnosticLog.ResultStatus.FAILED,
                    "Error while reading WebSubHub event publisher response.");
            log.debug("Error while reading WebSubHub event publisher response.", e);
        }
    }
}
