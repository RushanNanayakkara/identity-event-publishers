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

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.carbon.identity.subscription.management.api.model.Subscription;
import org.wso2.carbon.identity.subscription.management.api.model.SubscriptionStatus;
import org.wso2.carbon.identity.subscription.management.api.model.WebhookSubscriptionRequest;
import org.wso2.carbon.identity.subscription.management.api.model.WebhookUnsubscriptionRequest;
import org.wso2.identity.event.websubhub.publisher.internal.ClientManager;
import org.wso2.identity.event.websubhub.publisher.internal.WebSubHubAdapterDataHolder;
import org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil;

import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WebSubEventSubscriberImplTest {

    private WebSubEventSubscriberImpl subscriberService;
    private AutoCloseable mocks;

    @Mock
    private ClientManager mockClientManager;

    @Mock
    private CloseableHttpResponse mockHttpResponse;

    private MockedStatic<WebSubHubAdapterDataHolder> mockedStaticDataHolder;
    private MockedStatic<WebSubHubAdapterUtil> mockedStaticUtil;

    @BeforeMethod
    public void setUp() {

        mocks = MockitoAnnotations.openMocks(this);
        subscriberService = new WebSubEventSubscriberImpl();

        mockedStaticDataHolder = mockStatic(WebSubHubAdapterDataHolder.class);
        WebSubHubAdapterDataHolder mockDataHolder = mock(WebSubHubAdapterDataHolder.class);
        when(mockDataHolder.getClientManager()).thenReturn(mockClientManager);
        mockedStaticDataHolder.when(WebSubHubAdapterDataHolder::getInstance).thenReturn(mockDataHolder);

        mockedStaticUtil = mockStatic(WebSubHubAdapterUtil.class);
        mockedStaticUtil.when(WebSubHubAdapterUtil::getWebSubBaseURL).thenReturn("https://mock-websub-hub.com");
        mockedStaticUtil.when(() -> WebSubHubAdapterUtil.constructHubTopic(any(), any(), any(), any()))
                .thenAnswer(invocation -> invocation.getArgument(1) + "-" + invocation.getArgument(0));
    }

    @AfterMethod
    public void tearDown() throws Exception {

        if (mocks != null) {
            mocks.close();
        }
        if (mockedStaticDataHolder != null) {
            mockedStaticDataHolder.close();
        }
        if (mockedStaticUtil != null) {
            mockedStaticUtil.close();
        }
    }

    @Test
    public void testSubscribeSuccess() throws Exception {

        WebhookSubscriptionRequest request = WebhookSubscriptionRequest.builder()
                .channelsToSubscribe(Arrays.asList("topic1", "topic2"))
                .eventProfileVersion("v1")
                .endpoint("http://test-callback.com")
                .secret("secret")
                .build();

        HttpPost mockHttpPost = mock(HttpPost.class);
        when(mockClientManager.createHttpPost(any(), any(), any())).thenReturn(mockHttpPost);
        when(mockClientManager.executeSubscriberRequest(any())).thenReturn(mockHttpResponse);

        StatusLine mockStatusLine = mock(StatusLine.class);
        when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(202); // SC_ACCEPTED
        when(mockStatusLine.getReasonPhrase()).thenReturn("Accepted");
        when(mockHttpResponse.getEntity()).thenReturn(mock(HttpEntity.class));

        List<Subscription> result = subscriberService.subscribe(request, "tenant1");

        verify(mockClientManager, times(2)).executeSubscriberRequest(any());
        Assert.assertEquals(result.size(), 2);
        Assert.assertEquals(result.get(0).getStatus(), SubscriptionStatus.SUBSCRIPTION_ACCEPTED);
        Assert.assertEquals(result.get(1).getStatus(), SubscriptionStatus.SUBSCRIPTION_ACCEPTED);
    }

    @Test
    public void testUnsubscribeSuccess() throws Exception {

        WebhookUnsubscriptionRequest request = WebhookUnsubscriptionRequest.builder()
                .channelsToUnsubscribe(Arrays.asList("topic1", "topic2"))
                .eventProfileVersion("v1")
                .endpoint("http://test-callback.com")
                .build();

        HttpPost mockHttpPost = mock(HttpPost.class);
        when(mockClientManager.createHttpPost(any(), any(), any())).thenReturn(mockHttpPost);
        when(mockClientManager.executeSubscriberRequest(any())).thenReturn(mockHttpResponse);

        StatusLine mockStatusLine = mock(StatusLine.class);
        when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(202); // SC_ACCEPTED
        when(mockStatusLine.getReasonPhrase()).thenReturn("Accepted");
        when(mockHttpResponse.getEntity()).thenReturn(mock(HttpEntity.class));

        List<Subscription> result = subscriberService.unsubscribe(request, "tenant1");

        verify(mockClientManager, times(2)).executeSubscriberRequest(any());
        Assert.assertEquals(result.size(), 2);
        Assert.assertEquals(result.get(0).getStatus(), SubscriptionStatus.UNSUBSCRIPTION_ACCEPTED);
        Assert.assertEquals(result.get(1).getStatus(), SubscriptionStatus.UNSUBSCRIPTION_ACCEPTED);
    }
}
