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

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.carbon.identity.topic.management.api.exception.TopicManagementException;
import org.wso2.identity.event.websubhub.publisher.config.WebSubAdapterConfiguration;
import org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterClientException;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterException;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterServerException;
import org.wso2.identity.event.websubhub.publisher.internal.ClientManager;
import org.wso2.identity.event.websubhub.publisher.internal.WebSubHubAdapterDataHolder;
import org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil;
import org.wso2.identity.event.websubhub.publisher.util.WebSubHubCorrelationLogUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_REGISTERING_HUB_TOPIC;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.ACCEPTED;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.HUB_MODE;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME;

/**
 * Unit tests for the WebSubTopicManagerImpl class.
 */
public class WebSubTopicManagerImplTest {

    private WebSubTopicManagerImpl webSubTopicManager;
    private AutoCloseable mocks;

    @Mock
    private ClientManager mockClientManager;

    @Mock
    private WebSubAdapterConfiguration mockAdapterConfiguration;

    @Mock
    private CloseableHttpResponse mockHttpResponse;

    @Mock
    private HttpEntity mockEntity;

    @Mock
    private StatusLine mockStatusLine;

    @Mock
    private HttpPost mockHttpPost;

    private MockedStatic<WebSubHubAdapterDataHolder> mockedStaticDataHolder;
    private MockedStatic<WebSubHubAdapterUtil> mockedStaticUtil;
    private MockedStatic<WebSubHubCorrelationLogUtils> mockedStaticCorrelationLogUtils;

    @BeforeMethod
    public void setUp() throws WebSubAdapterException, IOException {

        mocks = MockitoAnnotations.openMocks(this);
        webSubTopicManager = new WebSubTopicManagerImpl();

        mockedStaticDataHolder = mockStatic(WebSubHubAdapterDataHolder.class);
        WebSubHubAdapterDataHolder mockDataHolder = mock(WebSubHubAdapterDataHolder.class);
        when(mockDataHolder.getClientManager()).thenReturn(mockClientManager);
        when(mockDataHolder.getAdapterConfiguration()).thenReturn(mockAdapterConfiguration);
        mockedStaticDataHolder.when(WebSubHubAdapterDataHolder::getInstance).thenReturn(mockDataHolder);

        mockedStaticUtil = mockStatic(WebSubHubAdapterUtil.class);
        mockedStaticUtil.when(WebSubHubAdapterUtil::getWebSubBaseURL).thenReturn("https://hub.example.com");
        mockedStaticUtil.when(() -> WebSubHubAdapterUtil.buildURL(anyString(), anyString(), anyString()))
                .thenReturn("https://hub.example.com?hub.mode=register&hub.topic=test-topic");
        mockedStaticUtil.when(
                        () -> WebSubHubAdapterUtil.constructHubTopic(anyString(), anyString(), anyString(),
                                anyString()))
                .thenAnswer(invocation -> {
                    String channelUri = invocation.getArgument(0);
                    String profileName = invocation.getArgument(1);
                    String profileVersion = invocation.getArgument(2);
                    String tenantDomain = invocation.getArgument(3);
                    return tenantDomain + "." + profileName + "." + profileVersion + "." + channelUri;
                });

        mockedStaticCorrelationLogUtils = mockStatic(WebSubHubCorrelationLogUtils.class);
        mockedStaticCorrelationLogUtils.when(() ->
                        WebSubHubCorrelationLogUtils.triggerCorrelationLogForRequest(any(HttpPost.class)))
                .thenAnswer(invocation -> null);

        mockedStaticCorrelationLogUtils.when(() ->
                        WebSubHubCorrelationLogUtils.triggerCorrelationLogForResponse(
                                any(HttpPost.class), anyLong(), anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> null);

        when(mockClientManager.createHttpPost(anyString(), any(), any())).thenReturn(mockHttpPost);
        when(mockClientManager.execute(any(HttpPost.class))).thenReturn(mockHttpResponse);
        when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockHttpResponse.getEntity()).thenReturn(mockEntity);

        String responseContent = HUB_MODE + "=" + ACCEPTED;
        when(mockEntity.getContent()).thenReturn(
                new ByteArrayInputStream(responseContent.getBytes(StandardCharsets.UTF_8)));
    }

    @AfterMethod
    public void tearDown() throws Exception {

        if (mockedStaticCorrelationLogUtils != null) {
            mockedStaticCorrelationLogUtils.close();
        }
        if (mockedStaticUtil != null) {
            mockedStaticUtil.close();
        }
        if (mockedStaticDataHolder != null) {
            mockedStaticDataHolder.close();
        }
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    public void testGetName() {

        assertEquals(webSubTopicManager.getAssociatedAdapter(), WEB_SUB_HUB_ADAPTER_NAME);
    }

    @Test
    public void testConstructTopic() throws TopicManagementException {

        String topic = webSubTopicManager.constructTopic("channel-uri", "WSO2", "1.0", "carbon.super");
        assertEquals(topic, "carbon.super.WSO2.1.0.channel-uri");
        mockedStaticUtil.verify(() ->
                WebSubHubAdapterUtil.constructHubTopic("channel-uri", "WSO2", "1.0", "carbon.super"));
    }

    @Test
    public void testRegisterTopicSuccess() throws TopicManagementException, WebSubAdapterException, IOException {

        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        webSubTopicManager.registerTopic("test-topic", "carbon.super");
        verify(mockClientManager).createHttpPost(anyString(), any(), any());
        verify(mockClientManager).execute(any(HttpPost.class));
    }

    @Test
    public void testDeregisterTopicSuccess() throws TopicManagementException, WebSubAdapterException, IOException {

        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        webSubTopicManager.deregisterTopic("test-topic", "carbon.super");
        verify(mockClientManager).createHttpPost(anyString(), any(), any());
        verify(mockClientManager).execute(any(HttpPost.class));
        mockedStaticUtil.verify(WebSubHubAdapterUtil::getWebSubBaseURL);
        mockedStaticUtil.verify(
                () -> WebSubHubAdapterUtil.buildURL("test-topic", "https://hub.example.com", "deregister"));
    }

    @Test(expectedExceptions = TopicManagementException.class)
    public void testRegisterTopicFailure() throws TopicManagementException {

        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        when(mockStatusLine.getReasonPhrase()).thenReturn("Internal Server Error");

        mockedStaticUtil.when(() -> WebSubHubAdapterUtil.handleFailedOperation(
                        any(), anyString(), anyString(), eq(HttpStatus.SC_INTERNAL_SERVER_ERROR)))
                .thenThrow(new WebSubAdapterException("Error", "Failed operation"));

        mockedStaticUtil.when(() -> WebSubHubAdapterUtil.handleClientException(
                        eq(WebSubHubAdapterConstants.ErrorMessages.TOPIC_DEREGISTRATION_FAILURE_ACTIVE_SUBS),
                        any(), any()))
                .thenReturn(new WebSubAdapterClientException("Error message", "Description", "ERROR_CODE"));

        mockedStaticUtil.when(() -> WebSubHubAdapterUtil.handleServerException(
                        eq(WebSubHubAdapterConstants.ErrorMessages.ERROR_REGISTERING_HUB_TOPIC),
                        any(), any()))
                .thenReturn(new WebSubAdapterServerException("Error message", "Description"));

        mockedStaticUtil.when(
                        () -> WebSubHubAdapterUtil.handleTopicMgtException(
                                eq(ERROR_REGISTERING_HUB_TOPIC),
                                any(),
                                anyString(),
                                anyString()
                        ))
                .thenReturn(new TopicManagementException("Error", "Description", "CODE"));

        webSubTopicManager.registerTopic("test-topic", "carbon.super");
    }

    @Test(expectedExceptions = TopicManagementException.class)
    public void testDeregisterTopicWithActiveSubscribersFailure() throws TopicManagementException {

        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_FORBIDDEN);
        when(mockStatusLine.getReasonPhrase()).thenReturn("Forbidden");

        java.util.Map<String, String> responseMap = new java.util.HashMap<>();
        responseMap.put(WebSubHubAdapterConstants.Http.HUB_REASON,
                String.format(WebSubHubAdapterConstants.Http.ERROR_TOPIC_DEREG_FAILURE_ACTIVE_SUBS, "test-topic"));
        responseMap.put(WebSubHubAdapterConstants.Http.HUB_ACTIVE_SUBS, "3");

        mockedStaticUtil.when(() -> WebSubHubAdapterUtil.parseEventHubResponse(any()))
                .thenReturn(responseMap);

        mockedStaticUtil.when(() -> WebSubHubAdapterUtil.handleClientException(
                        eq(WebSubHubAdapterConstants.ErrorMessages.TOPIC_DEREGISTRATION_FAILURE_ACTIVE_SUBS),
                        any(), any()))
                .thenReturn(new WebSubAdapterClientException("Error message", "Description", "ERROR_CODE"));

        mockedStaticUtil.when(() -> WebSubHubAdapterUtil.handleServerException(
                        eq(WebSubHubAdapterConstants.ErrorMessages.ERROR_REGISTERING_HUB_TOPIC),
                        any(), any()))
                .thenReturn(new WebSubAdapterServerException("Error message", "Description"));

        mockedStaticUtil.when(() -> WebSubHubAdapterUtil.handleTopicMgtException(any(), any(), any(), any()))
                .thenReturn(new TopicManagementException("Error", "Description", "CODE"));

        webSubTopicManager.deregisterTopic("test-topic", "carbon.super");
    }
}
