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

package org.wso2.identity.event.websubhub.publisher.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.MDC;
import org.wso2.carbon.identity.subscription.management.api.model.Subscription;
import org.wso2.carbon.identity.subscription.management.api.model.SubscriptionStatus;
import org.wso2.carbon.identity.subscription.management.api.model.WebhookSubscriptionRequest;
import org.wso2.carbon.identity.subscription.management.api.model.WebhookUnsubscriptionRequest;
import org.wso2.carbon.identity.subscription.management.api.service.EventSubscriber;
import org.wso2.carbon.utils.DiagnosticLog;
import org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterException;
import org.wso2.identity.event.websubhub.publisher.internal.ClientManager;
import org.wso2.identity.event.websubhub.publisher.internal.WebSubHubAdapterDataHolder;
import org.wso2.identity.event.websubhub.publisher.util.WebSubHubCorrelationLogUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.CORRELATION_ID_MDC;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_SUBSCRIBING_TO_TOPIC;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.HUB_CALLBACK;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.HUB_MODE;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.HUB_SECRET;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.HUB_TOPIC;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.constructHubTopic;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.getWebSubBaseURL;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleErrorResponse;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleFailedOperation;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleServerException;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleSuccessfulOperation;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.printSubscriberDiagnosticLog;

/**
 * OSGi service for managing WebSubHub subscriptions.
 * TODO: Add diagnostic logs
 */
public class WebSubEventSubscriberImpl implements EventSubscriber {

    private static final Log log = LogFactory.getLog(WebSubEventSubscriberImpl.class);

    @Override
    public String getAssociatedAdapter() {

        return WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME;
    }

    @Override
    public List<Subscription> subscribe(WebhookSubscriptionRequest webhookSubscriptionRequest, String tenantDomain) {

        List<Subscription> subscriptions = new ArrayList<>();
        for (String channelToSubscribe : webhookSubscriptionRequest.getChannelsToSubscribe()) {
            try {
                makeSubscriptionAPICall(channelToSubscribe,
                        constructHubTopic(channelToSubscribe, webhookSubscriptionRequest.getEventProfileName(),
                                webhookSubscriptionRequest.getEventProfileVersion(),
                                tenantDomain), getWebSubBaseURL(), WebSubHubAdapterConstants.Http.SUBSCRIBE,
                        webhookSubscriptionRequest.getEndpoint(), webhookSubscriptionRequest.getSecret());
                log.debug("WebSubHub subscription successful for channel: " + channelToSubscribe +
                        " with endpoint: " + webhookSubscriptionRequest.getEndpoint() + " in tenant: " +
                        tenantDomain);

                Subscription subscription = Subscription.builder()
                        .channelUri(channelToSubscribe)
                        .status(SubscriptionStatus.SUBSCRIPTION_ACCEPTED)
                        .build();
                subscriptions.add(subscription);
            } catch (WebSubAdapterException e) {
                log.debug("Error subscribing to channel" + channelToSubscribe + " with endpoint: " +
                        webhookSubscriptionRequest.getEndpoint() + " in tenant: " + tenantDomain + ". Error: " +
                        e.getMessage(), e);
                Subscription subscription = Subscription.builder()
                        .channelUri(channelToSubscribe)
                        .status(SubscriptionStatus.SUBSCRIPTION_ERROR)
                        .build();
                subscriptions.add(subscription);
            }
        }
        return subscriptions;
    }

    @Override
    public List<Subscription> unsubscribe(WebhookUnsubscriptionRequest webhookUnsubscriptionRequest,
                                          String tenantDomain) {

        List<Subscription> unsubscriptions = new ArrayList<>();
        for (String channelToUnsubscribe : webhookUnsubscriptionRequest.getChannelToUnsubscribe()) {
            try {
                makeSubscriptionAPICall(channelToUnsubscribe,
                        constructHubTopic(channelToUnsubscribe, webhookUnsubscriptionRequest.getEventProfileName(),
                                webhookUnsubscriptionRequest.getEventProfileVersion(),
                                tenantDomain), getWebSubBaseURL(), WebSubHubAdapterConstants.Http.UNSUBSCRIBE,
                        webhookUnsubscriptionRequest.getEndpoint(), null);
                log.debug("WebSubHub unsubscription successful for channel: " + channelToUnsubscribe +
                        " with endpoint: " + webhookUnsubscriptionRequest.getEndpoint() + " in tenant: " +
                        tenantDomain);

                Subscription unsubscription = Subscription.builder()
                        .channelUri(channelToUnsubscribe)
                        .status(SubscriptionStatus.UNSUBSCRIPTION_ACCEPTED)
                        .build();
                unsubscriptions.add(unsubscription);
            } catch (WebSubAdapterException e) {
                log.debug("Error unsubscribing from channel: " + channelToUnsubscribe +
                        " with endpoint: " + webhookUnsubscriptionRequest.getEndpoint() + " in tenant: " +
                        tenantDomain + ". Error: " + e.getMessage(), e);
                Subscription unsubscription = Subscription.builder()
                        .channelUri(channelToUnsubscribe)
                        .status(SubscriptionStatus.UNSUBSCRIPTION_ERROR)
                        .build();
                unsubscriptions.add(unsubscription);
            }
        }
        return unsubscriptions;
    }

    private void makeSubscriptionAPICall(String channelToSubscribe, String topic, String webSubHubBaseUrl,
                                         String operation, String callbackUrl,
                                         String secret) throws WebSubAdapterException {

        ClientManager clientManager = WebSubHubAdapterDataHolder.getInstance().getClientManager();

        HttpPost httpPost = clientManager.createHttpPost(webSubHubBaseUrl, null,
                Optional.ofNullable(MDC.get(CORRELATION_ID_MDC)).orElse(""));
        httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");

        List<BasicNameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair(HUB_CALLBACK, callbackUrl));
        params.add(new BasicNameValuePair(HUB_MODE, operation));
        params.add(new BasicNameValuePair(HUB_TOPIC, topic));
        if (secret != null) {
            params.add(new BasicNameValuePair(HUB_SECRET, secret));
        }
        httpPost.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));

        WebSubHubCorrelationLogUtils.triggerCorrelationLogForRequest(httpPost);
        final long requestStartTime = System.currentTimeMillis();

        printSubscriberDiagnosticLog(channelToSubscribe, callbackUrl, operation,
                DiagnosticLog.ResultStatus.SUCCESS, "Initiated " + operation + " channel request to WebSubHub.");

        try (CloseableHttpResponse response = (CloseableHttpResponse) clientManager.executeSubscriberRequest(
                httpPost)) {
            handleSubscriptionResponse(response, httpPost, topic, operation, requestStartTime);
            printSubscriberDiagnosticLog(channelToSubscribe, callbackUrl, operation,
                    DiagnosticLog.ResultStatus.SUCCESS, "Successful " + operation + " channel request to WebSubHub.");
        } catch (IOException | WebSubAdapterException e) {
            log.debug("Error subscribing to topic: " + topic + ". Error: " + e.getMessage(), e);
            printSubscriberDiagnosticLog(channelToSubscribe, callbackUrl, operation,
                    DiagnosticLog.ResultStatus.FAILED, "Failed " + operation + " channel request to WebSubHub.");
            throw handleServerException(ERROR_SUBSCRIBING_TO_TOPIC, e);
        }
    }

    private void handleSubscriptionResponse(CloseableHttpResponse response, HttpPost httpPost,
                                            String topic, String operation, long requestStartTime)
            throws IOException, WebSubAdapterException {

        StatusLine statusLine = response.getStatusLine();
        int responseCode = statusLine.getStatusCode();
        String responsePhrase = statusLine.getReasonPhrase();

        if (responseCode == HttpStatus.SC_ACCEPTED) {
            HttpEntity entity = response.getEntity();
            WebSubHubCorrelationLogUtils.triggerCorrelationLogForResponse(httpPost, requestStartTime,
                    WebSubHubCorrelationLogUtils.RequestStatus.COMPLETED.getStatus(),
                    String.valueOf(responseCode), responsePhrase);
            handleSuccessfulOperation(entity, topic, operation);
        } else if (responseCode == HttpStatus.SC_NOT_FOUND) {
            HttpEntity entity = response.getEntity();
            WebSubHubCorrelationLogUtils.triggerCorrelationLogForResponse(httpPost, requestStartTime,
                    WebSubHubCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                    String.valueOf(responseCode), responsePhrase);
            handleErrorResponse(entity, topic, operation);
        } else {
            WebSubHubCorrelationLogUtils.triggerCorrelationLogForResponse(httpPost, requestStartTime,
                    WebSubHubCorrelationLogUtils.RequestStatus.CANCELLED.getStatus(),
                    String.valueOf(responseCode), responsePhrase);
            HttpEntity entity = response.getEntity();
            handleFailedOperation(entity, topic, operation, responseCode);
        }
    }
}
