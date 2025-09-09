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

import org.apache.http.client.methods.HttpPost;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.carbon.identity.central.log.mgt.utils.LoggerUtils;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.carbon.identity.organization.management.service.OrganizationManager;
import org.wso2.carbon.identity.organization.management.service.exception.OrganizationManagementException;
import org.wso2.carbon.identity.topic.management.api.exception.TopicManagementException;
import org.wso2.carbon.utils.DiagnosticLog;
import org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterClientException;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterServerException;
import org.wso2.identity.event.websubhub.publisher.internal.WebSubHubAdapterDataHolder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Test class for WebSubHubAdapterUtil.
 */
public class WebSubHubAdapterUtilTest {

    @Test
    public void testGetCorrelationID() {

        SecurityEventTokenPayload payload = SecurityEventTokenPayload.builder()
                .iss("issuer")
                .jti("jti-token")
                .iat(System.currentTimeMillis())
                .aud("audience")
                .rci("test-correlation-id")
                .build();
        String correlationId = WebSubHubAdapterUtil.getCorrelationID(payload);
        Assert.assertNotNull(correlationId, "Correlation ID should not be null.");
        Assert.assertEquals(correlationId, "test-correlation-id");
    }

    @Test
    public void testHandleClientException() {

        WebSubAdapterClientException exception = WebSubHubAdapterUtil.handleClientException(
                WebSubHubAdapterConstants.ErrorMessages.ERROR_NULL_EVENT_PAYLOAD);
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.getMessage().contains("Invalid event payload input"));
    }

    @Test
    public void testHandleClientExceptionWithData() {

        WebSubAdapterClientException exception = WebSubHubAdapterUtil.handleClientException(
                WebSubHubAdapterConstants.ErrorMessages.ERROR_INVALID_RESPONSE_FROM_WEBSUB_HUB, "topic", "op", "resp");
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.getDescription().contains("topic"));
    }

    @Test
    public void testHandleServerException() {

        WebSubAdapterServerException exception = WebSubHubAdapterUtil.handleServerException(
                WebSubHubAdapterConstants.ErrorMessages.ERROR_CREATING_ASYNC_HTTP_CLIENT,
                new Exception("Test Exception"));
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.getMessage().contains("Error while creating the Async HTTP client."));
    }

    @Test
    public void testHandleServerExceptionWithData() {

        WebSubAdapterServerException exception = WebSubHubAdapterUtil.handleServerException(
                WebSubHubAdapterConstants.ErrorMessages.ERROR_INVALID_RESPONSE_FROM_WEBSUB_HUB,
                new Exception("Test"), "topic", "op", "resp");
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.getDescription().contains("topic"));
    }

    @Test
    public void testHandleTopicMgtException() {

        TopicManagementException ex = WebSubHubAdapterUtil.handleTopicMgtException(
                WebSubHubAdapterConstants.ErrorMessages.ERROR_REGISTERING_HUB_TOPIC,
                new Exception("ex"), "topic", "tenant");
        Assert.assertNotNull(ex);
    }

    @Test
    public void testBuildURLSuccess() throws Exception {

        String url = WebSubHubAdapterUtil
                .buildURL("topic", "http://localhost:8080/hub", "register");
        Assert.assertTrue(url.contains("hub.mode=register"));
        Assert.assertTrue(url.contains("hub.topic=topic"));
    }

    @Test(expectedExceptions = WebSubAdapterServerException.class)
    public void testBuildURLFailure() throws Exception {

        WebSubHubAdapterUtil.buildURL("topic", "://invalid-url", "register");
    }

    @Test
    public void testConstructHubTopic() throws Exception {

        try (MockedStatic<WebSubHubAdapterDataHolder> mockedHolder = mockStatic(WebSubHubAdapterDataHolder.class)) {
            WebSubHubAdapterDataHolder holder = mock(WebSubHubAdapterDataHolder.class);
            OrganizationManager orgManager = mock(OrganizationManager.class);
            when(holder.getOrganizationManager()).thenReturn(orgManager);
            when(orgManager.resolveOrganizationId(any())).thenReturn("orgid");
            mockedHolder.when(WebSubHubAdapterDataHolder::getInstance).thenReturn(holder);

            String topic = WebSubHubAdapterUtil.constructHubTopic(
                    "/event/channel", "WSO2", "v1", "tenant.com");
            Assert.assertTrue(topic.contains("tenant.com"));
            Assert.assertTrue(topic.contains("orgid"));
            Assert.assertTrue(topic.contains("wso2"));
            Assert.assertTrue(topic.contains("v1"));
            Assert.assertTrue(topic.contains("event"));
        }
    }

    @Test(expectedExceptions = WebSubAdapterServerException.class)
    public void testConstructHubTopicOrgIdFailure() throws Exception {

        try (MockedStatic<WebSubHubAdapterDataHolder> mockedHolder = mockStatic(WebSubHubAdapterDataHolder.class)) {
            WebSubHubAdapterDataHolder holder = mock(WebSubHubAdapterDataHolder.class);
            OrganizationManager orgManager = mock(OrganizationManager.class);
            when(holder.getOrganizationManager()).thenReturn(orgManager);
            when(orgManager.resolveOrganizationId(any())).thenThrow(new OrganizationManagementException("fail"));
            mockedHolder.when(WebSubHubAdapterDataHolder::getInstance).thenReturn(holder);

            WebSubHubAdapterUtil.constructHubTopic("/event/channel", "WSO2",
                    "v1", "tenant.com");
        }
    }

    @Test
    public void testPrintPublisherDiagnosticLog() {

        try (MockedStatic<LoggerUtils> mockedLogger = mockStatic(LoggerUtils.class)) {
            mockedLogger.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(true);
            mockedLogger.when(() -> LoggerUtils.triggerDiagnosticLogEvent(any()))
                    .then(invocation -> null);
            WebSubHubAdapterUtil.printPublisherDiagnosticLog("profile", "uri",
                    "events", "action",
                    DiagnosticLog.ResultStatus.SUCCESS, "message");
            mockedLogger.verify(() -> LoggerUtils.triggerDiagnosticLogEvent(any()), times(1));
        }
    }

    @Test
    public void testPrintSubscriberDiagnosticLog() {

        try (MockedStatic<LoggerUtils> mockedLogger = mockStatic(LoggerUtils.class)) {
            mockedLogger.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(true);
            mockedLogger.when(() -> LoggerUtils.triggerDiagnosticLogEvent(any()))
                    .then(invocation -> null);
            WebSubHubAdapterUtil.printSubscriberDiagnosticLog("channel",
                    "callback", "action",
                    DiagnosticLog.ResultStatus.SUCCESS, "message");
            mockedLogger.verify(() -> LoggerUtils.triggerDiagnosticLogEvent(any()), times(1));
        }
    }

    @Test
    public void testPrintTopicManagerDiagnosticLog() {

        try (MockedStatic<LoggerUtils> mockedLogger = mockStatic(LoggerUtils.class)) {
            mockedLogger.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(true);
            mockedLogger.when(() -> LoggerUtils.triggerDiagnosticLogEvent(any()))
                    .then(invocation -> null);
            WebSubHubAdapterUtil.printTopicManagerDiagnosticLog("topic", "action",
                    DiagnosticLog.ResultStatus.SUCCESS, "message");
            mockedLogger.verify(() -> LoggerUtils.triggerDiagnosticLogEvent(any()), times(1));
        }
    }

    @Test
    public void testHandleResponseCorrelationLog() {

        try (MockedStatic<WebSubHubCorrelationLogUtils> mockedLogUtils = mockStatic(
                WebSubHubCorrelationLogUtils.class)) {
            HttpPost post = mock(HttpPost.class);
            mockedLogUtils.when(
                            () -> WebSubHubCorrelationLogUtils
                                    .triggerCorrelationLogForResponse(any(), anyLong(), any()))
                    .then(invocation -> null);
            WebSubHubAdapterUtil
                    .handleResponseCorrelationLog(post, 123L, "param1", "param2");
            mockedLogUtils.verify(
                    () -> WebSubHubCorrelationLogUtils.triggerCorrelationLogForResponse(any(), anyLong(), any()),
                    times(1));
        }
    }

    @Test(expectedExceptions = WebSubAdapterServerException.class)
    public void testHandleSuccessfulOperationNullEntity() throws Exception {

        WebSubHubAdapterUtil.handleSuccessfulOperation(null, "topic", "op");
    }
}
