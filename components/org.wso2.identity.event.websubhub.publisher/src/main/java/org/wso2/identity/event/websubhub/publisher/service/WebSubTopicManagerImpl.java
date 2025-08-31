/*
 * Copyright (c) 2024, WSO2 LLC. (http://www.wso2.com).
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
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.slf4j.MDC;
import org.wso2.carbon.identity.topic.management.api.exception.TopicManagementException;
import org.wso2.carbon.identity.topic.management.api.service.TopicManager;
import org.wso2.carbon.utils.DiagnosticLog;
import org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterException;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterServerException;
import org.wso2.identity.event.websubhub.publisher.internal.ClientManager;
import org.wso2.identity.event.websubhub.publisher.internal.WebSubHubAdapterDataHolder;
import org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil;
import org.wso2.identity.event.websubhub.publisher.util.WebSubHubCorrelationLogUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.CORRELATION_ID_MDC;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_BACKEND_ERROR_FROM_WEBSUB_HUB;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_DEREGISTERING_HUB_TOPIC;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_EMPTY_RESPONSE_FROM_WEBSUB_HUB;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_INVALID_RESPONSE_FROM_WEBSUB_HUB;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_REGISTERING_HUB_TOPIC;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.TOPIC_DEREGISTRATION_FAILURE_ACTIVE_SUBS;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.DEREGISTER;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.ERROR_TOPIC_DEREG_FAILURE_ACTIVE_SUBS;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.HUB_ACTIVE_SUBS;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.HUB_REASON;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.REGISTER;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.RESPONSE_FOR_SUCCESSFUL_OPERATION;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.buildURL;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.constructHubTopic;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.getWebSubBaseURL;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleClientException;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleServerException;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleTopicMgtException;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.parseEventHubResponse;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.printTopicManagerDiagnosticLog;

/**
 * OSGi service for register topics using web sub hub.
 */
public class WebSubTopicManagerImpl implements TopicManager {

    private static final Log log = LogFactory.getLog(WebSubTopicManagerImpl.class);
    private static final int MAX_RETRIES = 2;
    private static final long RETRY_DELAY_MS = 400;

    @Override
    public String getAssociatedAdapter() {

        return WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME;
    }

    @Override
    public String constructTopic(String channelUri, String eventProfileName, String eventProfileVersion,
                                 String tenantDomain)
            throws TopicManagementException {

        try {
            return constructHubTopic(channelUri, eventProfileName, eventProfileVersion, tenantDomain);
        } catch (WebSubAdapterServerException e) {
            throw WebSubHubAdapterUtil.handleTopicMgtException(
                    WebSubHubAdapterConstants.ErrorMessages.ERROR_CONSTRUCTING_HUB_TOPIC, e, channelUri,
                    eventProfileVersion, tenantDomain);
        }
    }

    @Override
    public void registerTopic(String topic, String tenantDomain) throws TopicManagementException {

        try {
            makeTopicMgtAPICall(topic, getWebSubBaseURL(),
                    WebSubHubAdapterConstants.Http.REGISTER, tenantDomain);
            log.debug("WebSubHub Topic registered successfully for the topic: " + topic + " in tenant: " +
                    tenantDomain);
        } catch (WebSubAdapterException e) {
            throw handleTopicMgtException(ERROR_REGISTERING_HUB_TOPIC, e, topic, tenantDomain);
        }
    }

    @Override
    public void deregisterTopic(String topic, String tenantDomain) throws TopicManagementException {

        try {
            makeTopicMgtAPICall(topic, getWebSubBaseURL(),
                    WebSubHubAdapterConstants.Http.DEREGISTER, tenantDomain);
            log.debug("WebSubHub Topic deregistered successfully for the topic: " + topic + " in tenant: " +
                    tenantDomain);
        } catch (WebSubAdapterException e) {
            throw handleTopicMgtException(ERROR_DEREGISTERING_HUB_TOPIC, e, topic, tenantDomain);
        }
    }

    private void makeTopicMgtAPICall(String topic, String webSubHubBaseUrl, String operation, String tenantDomain)
            throws WebSubAdapterException {

        String topicMgtUrl = buildURL(topic, webSubHubBaseUrl, operation);
        ClientManager clientManager = WebSubHubAdapterDataHolder.getInstance().getClientManager();

        int attempt = 0;
        while (true) {
            HttpPost httpPost = clientManager.createHttpPost(topicMgtUrl, null, MDC.get(CORRELATION_ID_MDC));
            httpPost.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

            WebSubHubCorrelationLogUtils.triggerCorrelationLogForRequest(httpPost);
            final long requestStartTime = System.currentTimeMillis();

            printTopicManagerDiagnosticLog(topic, operation,
                    DiagnosticLog.ResultStatus.SUCCESS, "Initiated topic " + operation + " request to WebSubHub.");

            try (CloseableHttpResponse response = (CloseableHttpResponse) clientManager.execute(httpPost)) {

                StatusLine statusLine = response.getStatusLine();
                int responseCode = statusLine.getStatusCode();
                String responsePhrase = statusLine.getReasonPhrase();

                if (responseCode >= 500 && responseCode < 600 && attempt < MAX_RETRIES) {
                    attempt++;
                    log.debug("Retrying topic management API call, attempt " + attempt + " for topic: " + topic);
                    Thread.sleep(RETRY_DELAY_MS);
                    printTopicManagerDiagnosticLog(topic, operation,
                            DiagnosticLog.ResultStatus.SUCCESS,
                            "Retrying topic " + operation + " request to WebSubHub. due to " +
                                    "server error, attempt: " + attempt);
                    continue;
                }

                if (responseCode == HttpStatus.SC_OK) {
                    HttpEntity entity = response.getEntity();
                    WebSubHubCorrelationLogUtils.triggerCorrelationLogForResponse(httpPost, requestStartTime,
                            WebSubHubCorrelationLogUtils.RequestStatus.COMPLETED.getStatus(),
                            String.valueOf(responseCode), responsePhrase);
                    if (entity != null) {
                        String responseString = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                        if (RESPONSE_FOR_SUCCESSFUL_OPERATION.equals(responseString)) {
                            log.debug("Success WebSub Hub operation: " + operation + ", topic: " + topic);
                            printTopicManagerDiagnosticLog(topic, operation,
                                    DiagnosticLog.ResultStatus.SUCCESS,
                                    "Success topic " + operation + " request to WebSubHub.");
                        } else {
                            printTopicManagerDiagnosticLog(topic, operation,
                                    DiagnosticLog.ResultStatus.FAILED,
                                    "Received invalid response to topic " + operation + " request to WebSubHub.");
                            throw handleServerException(ERROR_INVALID_RESPONSE_FROM_WEBSUB_HUB, null, topic,
                                    operation, responseString);
                        }
                    } else {
                        String message =
                                String.format(ERROR_EMPTY_RESPONSE_FROM_WEBSUB_HUB.getDescription(), topic, operation);
                        printTopicManagerDiagnosticLog(topic, operation,
                                DiagnosticLog.ResultStatus.FAILED,
                                "Received empty response to topic " + operation + " request to WebSubHub.");
                        throw new WebSubAdapterServerException(message, ERROR_EMPTY_RESPONSE_FROM_WEBSUB_HUB.getCode());
                    }
                } else if ((responseCode == HttpStatus.SC_CONFLICT && operation.equals(REGISTER)) ||
                        (responseCode == HttpStatus.SC_NOT_FOUND && operation.equals(DEREGISTER))) {
                    HttpEntity entity = response.getEntity();
                    String responseString = "";
                    WebSubHubCorrelationLogUtils.triggerCorrelationLogForResponse(httpPost, requestStartTime,
                            WebSubHubCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                            String.valueOf(responseCode), responsePhrase);
                    if (entity != null) {
                        responseString = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug(String.format(ERROR_INVALID_RESPONSE_FROM_WEBSUB_HUB.getDescription(),
                                topic, operation, responseString));
                    }
                } else {
                    WebSubHubCorrelationLogUtils.triggerCorrelationLogForResponse(httpPost, requestStartTime,
                            WebSubHubCorrelationLogUtils.RequestStatus.CANCELLED.getStatus(),
                            String.valueOf(responseCode), responsePhrase);
                    if (responseCode == HttpStatus.SC_FORBIDDEN) {
                        Map<String, String> hubResponse = parseEventHubResponse(response);
                        if (!hubResponse.isEmpty() && hubResponse.containsKey(HUB_REASON)) {
                            String errorMsg = String.format(ERROR_TOPIC_DEREG_FAILURE_ACTIVE_SUBS, topic);
                            if (errorMsg.equals(hubResponse.get(HUB_REASON))) {
                                log.error(String.format(TOPIC_DEREGISTRATION_FAILURE_ACTIVE_SUBS.getDescription(),
                                        topic, hubResponse.get(HUB_ACTIVE_SUBS)));
                                printTopicManagerDiagnosticLog(topic, operation,
                                        DiagnosticLog.ResultStatus.FAILED,
                                        "Error occurred while processing topic " + operation +
                                                " request to WebSubHub.");
                                throw handleClientException(TOPIC_DEREGISTRATION_FAILURE_ACTIVE_SUBS, topic,
                                        hubResponse.get(HUB_ACTIVE_SUBS));
                            }
                        }
                    }
                    HttpEntity entity = response.getEntity();
                    String responseString = "";
                    if (entity != null) {
                        responseString = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                    }
                    String message = String.format(ERROR_BACKEND_ERROR_FROM_WEBSUB_HUB.getDescription(),
                            topic, operation, responseString);
                    log.error(message + ", Response code:" + responseCode);
                    printTopicManagerDiagnosticLog(topic, operation,
                            DiagnosticLog.ResultStatus.FAILED,
                            "Error occurred while executing topic " + operation +
                                    " request to WebSubHub.");
                    throw new WebSubAdapterServerException(message, ERROR_BACKEND_ERROR_FROM_WEBSUB_HUB.getCode());
                }

                break;
            } catch (IOException e) {

                if (attempt < MAX_RETRIES) {
                    attempt++;
                    log.debug("Retrying topic management API call due to network error, attempt " + attempt +
                            " for topic: " + topic);
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                        printTopicManagerDiagnosticLog(topic, operation,
                                DiagnosticLog.ResultStatus.SUCCESS,
                                "Retrying topic " + operation + " request to WebSubHub due to network error, attempt." +
                                        attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        printTopicManagerDiagnosticLog(topic, operation,
                                DiagnosticLog.ResultStatus.FAILED,
                                "Error occurred while executing topic " + operation +
                                        " request to WebSubHub.");
                        throw handleServerException(ERROR_REGISTERING_HUB_TOPIC, ie, topic, tenantDomain);
                    }
                    continue;
                }
                printTopicManagerDiagnosticLog(topic, operation,
                        DiagnosticLog.ResultStatus.FAILED,
                        "Error occurred while executing topic " + operation +
                                " request to WebSubHub.");
                throw handleServerException(ERROR_REGISTERING_HUB_TOPIC, e, topic, tenantDomain);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                printTopicManagerDiagnosticLog(topic, operation,
                        DiagnosticLog.ResultStatus.FAILED,
                        "Error occurred while executing topic " + operation +
                                " request to WebSubHub.");
                throw handleServerException(ERROR_REGISTERING_HUB_TOPIC, ie, topic, tenantDomain);
            }
        }
    }
}
