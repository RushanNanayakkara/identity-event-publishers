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

package org.wso2.identity.event.websubhub.publisher.util;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.wso2.carbon.identity.central.log.mgt.utils.LoggerUtils;
import org.wso2.carbon.identity.event.publisher.api.constant.ErrorMessage;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherException;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherServerException;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.carbon.identity.organization.management.service.exception.OrganizationManagementException;
import org.wso2.carbon.identity.topic.management.api.exception.TopicManagementException;
import org.wso2.carbon.utils.DiagnosticLog;
import org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterClientException;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterException;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterServerException;
import org.wso2.identity.event.websubhub.publisher.internal.WebSubHubAdapterDataHolder;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_BACKEND_ERROR_FROM_WEBSUB_HUB;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_EMPTY_RESPONSE_FROM_WEBSUB_HUB;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_INVALID_RESPONSE_FROM_WEBSUB_HUB;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_INVALID_WEB_SUB_HUB_BASE_URL;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.HUB_MODE;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.HUB_TOPIC;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.RESPONSE_FOR_SUCCESSFUL_OPERATION;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.URL_KEY_VALUE_SEPARATOR;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.URL_PARAM_SEPARATOR;

/**
 * Utility class for WebSubHubAdapter.
 */
public class WebSubHubAdapterUtil {

    private static final Log log = LogFactory.getLog(WebSubHubAdapterUtil.class);

    private WebSubHubAdapterUtil() {

    }

    /**
     * Get the correlation ID.
     *
     * @return Correlation ID.
     */
    public static String getCorrelationID(SecurityEventTokenPayload eventTokenPayload) {

        return eventTokenPayload.getRci();
    }

    /**
     * Handle client exceptions.
     *
     * @param error Error message.
     * @param data  Data.
     * @return WebSubAdapterClientException.
     */
    public static WebSubAdapterClientException handleClientException(
            WebSubHubAdapterConstants.ErrorMessages error, String... data) {

        String description = error.getDescription();
        if (ArrayUtils.isNotEmpty(data)) {
            description = String.format(description, data);
        }
        return new WebSubAdapterClientException(error.getMessage(), description, error.getCode());
    }

    /**
     * Handle server exceptions.
     *
     * @param error     Error message.
     * @param throwable Throwable.
     * @param data      Data.
     * @return WebSubAdapterServerException.
     */
    public static WebSubAdapterServerException handleServerException(
            WebSubHubAdapterConstants.ErrorMessages error, Throwable throwable, String... data) {

        String description = error.getDescription();
        if (ArrayUtils.isNotEmpty(data)) {
            description = String.format(description, data);
        }
        return new WebSubAdapterServerException(error.getMessage(), description, error.getCode(), throwable);
    }

    /**
     * Handle server exceptions.
     *
     * @param error     Error message.
     * @param throwable Throwable.
     * @param data      Data.
     * @return EventPublisherException.
     */
    public static EventPublisherException handleServerException(
            ErrorMessage error, Throwable throwable, String... data) {

        String description = error.getDescription();
        if (ArrayUtils.isNotEmpty(data)) {
            description = String.format(description, data);
        }
        return new EventPublisherServerException(error.getMessage(), description, error.getCode(), throwable);
    }

    /**
     * Handle server exceptions.
     *
     * @param error Error message.
     * @param data  Data.
     * @return EventPublisherException.
     */
    public static EventPublisherException handleServerException(
            ErrorMessage error, String... data) {

        String description = error.getDescription();
        if (ArrayUtils.isNotEmpty(data)) {
            description = String.format(description, data);
        }
        return new EventPublisherServerException(error.getMessage(), description, error.getCode());
    }

    /**
     * Handle topic management exceptions without a throwable.
     *
     * @param error Error message.
     * @param data  Data.
     * @return TopicManagementException.
     */
    public static TopicManagementException handleTopicMgtException(
            WebSubHubAdapterConstants.ErrorMessages error, Throwable throwable, String... data) {

        String description = error.getDescription();
        if (ArrayUtils.isNotEmpty(data)) {
            description = String.format(description, data);
        }
        return new TopicManagementException(error.getMessage(), description, error.getCode(), throwable);
    }

    /**
     * Parse the response from the event hub.
     *
     * @param response HTTP response.
     * @return Parsed response as a map.
     * @throws IOException If an error occurs while reading the response.
     */
    public static Map<String, String> parseEventHubResponse(CloseableHttpResponse response) throws IOException {

        Map<String, String> map = new HashMap<>();
        HttpEntity entity = response.getEntity();

        if (entity != null) {
            String responseContent = EntityUtils.toString(entity, StandardCharsets.UTF_8);
            log.debug("Parsing response content from event hub: " + responseContent);
            String[] responseParams = responseContent.split(URL_PARAM_SEPARATOR);
            for (String param : responseParams) {
                String[] keyValuePair = param.split(URL_KEY_VALUE_SEPARATOR);
                if (keyValuePair.length == 2) {
                    map.put(keyValuePair[0], keyValuePair[1]);
                }
            }
        }
        return map;
    }

    /**
     * Handle the response correlation log.
     *
     * @param request          HTTP POST request.
     * @param requestStartTime Request start time.
     * @param otherParams      Other parameters.
     */
    public static void handleResponseCorrelationLog(HttpPost request, long requestStartTime, String... otherParams) {

        WebSubHubCorrelationLogUtils.triggerCorrelationLogForResponse(request, requestStartTime, otherParams);
    }

    /**
     * Print diagnostic log for publisher operations.
     *
     * @param eventProfileName Event profile name.
     * @param eventProfileUri  Event profile URI.
     * @param events           Events.
     * @param action           Action performed.
     * @param status           Result status.
     * @param message          Result message.
     */
    public static void printPublisherDiagnosticLog(String eventProfileName, String eventProfileUri, String events,
                                                   String action, DiagnosticLog.ResultStatus status, String message) {

        if (LoggerUtils.isDiagnosticLogsEnabled()) {
            DiagnosticLog.DiagnosticLogBuilder diagnosticLogBuilder = new DiagnosticLog.DiagnosticLogBuilder(
                    WebSubHubAdapterConstants.LogConstants.WEB_SUB_HUB_ADAPTER, action);
            diagnosticLogBuilder
                    .inputParam(WebSubHubAdapterConstants.LogConstants.InputKeys.EVENT_URI, eventProfileUri)
                    .inputParam(WebSubHubAdapterConstants.LogConstants.InputKeys.EVENT_PROFILE_NAME, eventProfileName)
                    .inputParam(WebSubHubAdapterConstants.LogConstants.InputKeys.EVENTS, events)
                    .resultMessage(message)
                    .resultStatus(status)
                    .logDetailLevel(DiagnosticLog.LogDetailLevel.APPLICATION);
            LoggerUtils.triggerDiagnosticLogEvent(diagnosticLogBuilder);
        }
    }

    /**
     * Print diagnostic log for subscriber operations.
     *
     * @param channelToSubscribe Channel to subscribe.
     * @param callbackUrl        Callback URL.
     * @param action             Action performed.
     * @param status             Result status.
     * @param message            Result message.
     */
    public static void printSubscriberDiagnosticLog(String channelToSubscribe, String callbackUrl, String action,
                                                    DiagnosticLog.ResultStatus status, String message) {

        if (LoggerUtils.isDiagnosticLogsEnabled()) {
            DiagnosticLog.DiagnosticLogBuilder diagnosticLogBuilder = new DiagnosticLog.DiagnosticLogBuilder(
                    WebSubHubAdapterConstants.LogConstants.WEB_SUB_HUB_ADAPTER, action);
            diagnosticLogBuilder
                    .inputParam(WebSubHubAdapterConstants.LogConstants.InputKeys.CHANNEL, channelToSubscribe)
                    .inputParam(WebSubHubAdapterConstants.LogConstants.InputKeys.ENDPOINT, callbackUrl)
                    .resultMessage(message)
                    .resultStatus(status)
                    .logDetailLevel(DiagnosticLog.LogDetailLevel.APPLICATION);
            LoggerUtils.triggerDiagnosticLogEvent(diagnosticLogBuilder);
        }
    }

    /**
     * Print diagnostic log for topic manager operations.
     *
     * @param topic   Topic.
     * @param action  Action performed.
     * @param status  Result status.
     * @param message Result message.
     */
    public static void printTopicManagerDiagnosticLog(String topic, String action,
                                                      DiagnosticLog.ResultStatus status, String message) {

        if (LoggerUtils.isDiagnosticLogsEnabled()) {
            DiagnosticLog.DiagnosticLogBuilder diagnosticLogBuilder = new DiagnosticLog.DiagnosticLogBuilder(
                    WebSubHubAdapterConstants.LogConstants.WEB_SUB_HUB_ADAPTER, action);
            diagnosticLogBuilder
                    .inputParam(WebSubHubAdapterConstants.LogConstants.InputKeys.TOPIC, topic)
                    .resultMessage(message)
                    .resultStatus(status)
                    .logDetailLevel(DiagnosticLog.LogDetailLevel.INTERNAL_SYSTEM);
            LoggerUtils.triggerDiagnosticLogEvent(diagnosticLogBuilder);
        }
    }

    /**
     * Build URL for WebSubHub operations.
     *
     * @param topic            Topic.
     * @param webSubHubBaseUrl WebSubHub base URL.
     * @param operation        Operation.
     * @return URL.
     * @throws WebSubAdapterServerException If an error occurs while building the URL.
     */
    public static String buildURL(String topic, String webSubHubBaseUrl, String operation)
            throws WebSubAdapterServerException {

        try {
            URIBuilder uriBuilder = new URIBuilder(webSubHubBaseUrl);
            uriBuilder.addParameter(HUB_MODE, operation);
            uriBuilder.addParameter(HUB_TOPIC, topic);
            return uriBuilder.build().toString();
        } catch (URISyntaxException e) {
            log.error("Error building URL", e);
            throw handleServerException(ERROR_INVALID_WEB_SUB_HUB_BASE_URL, e);
        }
    }

    /**
     * Construct the hub topic by combining tenant domain, version and topic suffix.
     *
     * @param channelUri          Channel URI.
     * @param eventProfileName    Event profile name.
     * @param eventProfileVersion Event profile version.
     * @param tenantDomain        Tenant domain.
     * @return Hub topic. Returns topic in the format: {tenantDomain}.{orgid}.schema.{schema}.{version}.event.{event}
     */
    public static String constructHubTopic(String channelUri, String eventProfileName, String eventProfileVersion,
                                           String tenantDomain)
            throws WebSubAdapterServerException {

        String event = extractEvent(channelUri);

        return tenantDomain + WebSubHubAdapterConstants.Http.TOPIC_SEPARATOR + getOrganizationId(tenantDomain) +
                WebSubHubAdapterConstants.Http.TOPIC_SEPARATOR +
                WebSubHubAdapterConstants.SCHEMA + WebSubHubAdapterConstants.Http.TOPIC_SEPARATOR +
                eventProfileName.toLowerCase() + WebSubHubAdapterConstants.Http.TOPIC_SEPARATOR +
                eventProfileVersion +
                WebSubHubAdapterConstants.Http.TOPIC_SEPARATOR + WebSubHubAdapterConstants.EVENT +
                WebSubHubAdapterConstants.Http.TOPIC_SEPARATOR + event;
    }

    private static String extractEvent(String channelUri) {

        int lastSlash = channelUri.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < channelUri.length() - 1) {
            return channelUri.substring(lastSlash + 1);
        }
        return "";
    }

    /**
     * Get the WebSubHub base URL from configuration.
     *
     * @return WebSubHub base URL.
     * @throws WebSubAdapterException If the WebSubHub base URL is not configured.
     */
    public static String getWebSubBaseURL() throws WebSubAdapterException {

        String webSubHubBaseUrl =
                WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration().getWebSubHubBaseUrl();

        if (StringUtils.isEmpty(webSubHubBaseUrl)) {
            throw handleClientException(WebSubHubAdapterConstants.ErrorMessages.WEB_SUB_BASE_URL_NOT_CONFIGURED);
        }
        return webSubHubBaseUrl;
    }

    /**
     * Handle successful WebSubHub operations.
     *
     * @param entity    HTTP entity.
     * @param topic     Topic.
     * @param operation Operation.
     * @throws WebSubAdapterException If an error occurs while handling the response.
     * @throws IOException            If an error occurs while reading the response.
     */
    public static void handleSuccessfulOperation(HttpEntity entity, String topic, String operation)
            throws WebSubAdapterException, IOException {

        if (entity != null) {
            String responseString = EntityUtils.toString(entity, StandardCharsets.UTF_8);
            if (RESPONSE_FOR_SUCCESSFUL_OPERATION.equals(responseString)) {
                log.debug("Success WebSubHub operation: " + operation + ", topic: " + topic);
            } else {
                throw handleServerException(ERROR_INVALID_RESPONSE_FROM_WEBSUB_HUB, null,
                        topic, operation, responseString);
            }
        } else {
            String message = String.format(ERROR_EMPTY_RESPONSE_FROM_WEBSUB_HUB.getDescription(), topic, operation);
            throw new WebSubAdapterServerException(message, ERROR_EMPTY_RESPONSE_FROM_WEBSUB_HUB.getCode());
        }
    }

    /**
     * Handle error responses (e.g., conflict or not found) from WebSubHub.
     *
     * @param entity    HTTP entity.
     * @param topic     Topic.
     * @param operation Operation.
     * @throws IOException If an error occurs while reading the response.
     */
    public static void handleErrorResponse(HttpEntity entity, String topic, String operation)
            throws IOException, WebSubAdapterServerException {

        String responseString = "";
        if (entity != null) {
            responseString = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        }
        throw handleServerException(ERROR_INVALID_RESPONSE_FROM_WEBSUB_HUB, null,
                topic, operation, responseString);
    }

    /**
     * Handle failed WebSubHub operations.
     *
     * @param entity       HTTP entity.
     * @param topic        Topic.
     * @param operation    Operation.
     * @param responseCode Response code.
     * @throws IOException            If an error occurs while reading the response.
     * @throws WebSubAdapterException If an error occurs while handling the response.
     */
    public static void handleFailedOperation(HttpEntity entity, String topic, String operation, int responseCode)
            throws IOException, WebSubAdapterException {

        String responseString = "";
        if (entity != null) {
            responseString = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        }
        String message = String.format(ERROR_BACKEND_ERROR_FROM_WEBSUB_HUB.getDescription(),
                topic, operation, responseString);
        log.error(message + ", Response code:" + responseCode);
        throw new WebSubAdapterServerException(message, ERROR_BACKEND_ERROR_FROM_WEBSUB_HUB.getCode());
    }

    /**
     * Get the organization id of the tenant.
     *
     * @param tenantDomain Tenant domain.
     * @return Organization id.
     * @throws WebSubAdapterServerException If an error occurs while resolving the organization id.
     */
    private static String getOrganizationId(String tenantDomain) throws WebSubAdapterServerException {

        String orgId;
        try {
            orgId = WebSubHubAdapterDataHolder.getInstance().getOrganizationManager()
                    .resolveOrganizationId(tenantDomain);
        } catch (OrganizationManagementException e) {
            throw handleServerException(
                    WebSubHubAdapterConstants.ErrorMessages.ERROR_RESOLVING_ORG_ID, e, tenantDomain);
        }
        return orgId;
    }
}
