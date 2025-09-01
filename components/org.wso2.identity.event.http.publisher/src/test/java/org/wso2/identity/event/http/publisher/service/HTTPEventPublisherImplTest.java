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

package org.wso2.identity.event.http.publisher.service;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.identity.central.log.mgt.utils.LoggerUtils;
import org.wso2.carbon.identity.core.util.IdentityTenantUtil;
import org.wso2.carbon.identity.event.publisher.api.model.EventContext;
import org.wso2.carbon.identity.event.publisher.api.model.EventPayload;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.carbon.identity.webhook.management.api.model.Webhook;
import org.wso2.identity.event.http.publisher.internal.component.ClientManager;
import org.wso2.identity.event.http.publisher.internal.component.HTTPAdapterDataHolder;
import org.wso2.identity.event.http.publisher.internal.service.impl.HTTPEventPublisherImpl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HTTPEventPublisherImplTest {

    private HTTPEventPublisherImpl adapterService;
    private AutoCloseable mocks;

    @Mock
    private ClientManager mockClientManager;

    @Mock
    private HttpResponse mockHttpResponse;

    private MockedStatic<HTTPAdapterDataHolder> mockedStaticDataHolder;
    private static MockedStatic<IdentityTenantUtil> mockedStaticIdentityTenantUtil;

    @BeforeClass
    public void setUp() throws Exception {

        mocks = MockitoAnnotations.openMocks(this);
        adapterService = spy(new HTTPEventPublisherImpl());
        mockIdentityTenantUtil();

        mockedStaticDataHolder = mockStatic(HTTPAdapterDataHolder.class);
        HTTPAdapterDataHolder mockDataHolder = mock(HTTPAdapterDataHolder.class);
        mockedStaticDataHolder.when(HTTPAdapterDataHolder::getInstance).thenReturn(mockDataHolder);

        when(mockDataHolder.getClientManager()).thenReturn(mockClientManager);

        // Mock active webhooks
        Webhook webhook1 = mock(Webhook.class);
        when(webhook1.getEndpoint()).thenReturn("http://mock-endpoint-1.com");
        when(webhook1.getSecret()).thenReturn("secret1");
        Webhook webhook2 = mock(Webhook.class);
        when(webhook2.getEndpoint()).thenReturn("http://mock-endpoint-2.com");
        when(webhook2.getSecret()).thenReturn("secret2");
        List<Webhook> webhooks = Arrays.asList(webhook1, webhook2);

        // Mock getActiveWebhooks to return our list
        when(mockDataHolder.getWebhookManagementService()).thenReturn(
                mock(org.wso2.carbon.identity.webhook.management.api.service.WebhookManagementService.class));
        when(mockDataHolder.getWebhookManagementService().getActiveWebhooks(any(), any(), any(), any()))
                .thenReturn(webhooks);

        EventContext eventContext = EventContext.builder()
                .tenantDomain("test-tenant")
                .eventProfileName("WSO2")
                .eventUri("test-uri")
                .eventProfileVersion("v1")
                .build();
        adapterService.canHandleEvent(eventContext);
    }

    @AfterClass
    public void tearDown() throws Exception {

        if (mocks != null) {
            mocks.close();
        }
        if (mockedStaticDataHolder != null) {
            mockedStaticDataHolder.close();
        }
    }

    @Test
    public void testPublishSuccess() throws Exception {

        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);

            EventContext eventContext = EventContext.builder()
                    .tenantDomain("test-tenant")
                    .eventProfileName("WSO2")
                    .eventUri("test-uri")
                    .build();
            SecurityEventTokenPayload payload = SecurityEventTokenPayload.builder()
                    .iss("issuer")
                    .jti("jti-token")
                    .iat(System.currentTimeMillis())
                    .aud("audience")
                    .events(Collections.singletonMap("event1", new EventPayload() {
                    }))
                    .build();

            // Mock ClientManager behavior to simulate success
            CompletableFuture<HttpResponse> future = CompletableFuture.completedFuture(mockHttpResponse);
            when(mockClientManager.executeAsync(any())).thenReturn(future);
            when(mockClientManager.createHttpPost(any(), any(), any())).thenReturn(
                    mock(HttpPost.class));
            when(mockClientManager.getAsyncCallbackExecutor()).thenReturn((Executor) Runnable::run);

            // Execute and verify no exception is thrown
            adapterService.publish(payload, eventContext);

            // Verify interactions for each webhook
            verify(mockClientManager, times(2)).executeAsync(any());
            verify(mockClientManager, times(2)).createHttpPost(any(), any(), any());
        }
    }

    /**
     * Mocks the IdentityTenantUtil.
     */
    private static void mockIdentityTenantUtil() {

        if (mockedStaticIdentityTenantUtil != null && !mockedStaticIdentityTenantUtil.isClosed()) {
            mockedStaticIdentityTenantUtil.close();
        }
        mockedStaticIdentityTenantUtil = mockStatic(IdentityTenantUtil.class);
        when(IdentityTenantUtil.isTenantedSessionsEnabled()).thenReturn(false);
        when(IdentityTenantUtil.getTenantId("test-tenant")).thenReturn(1);
    }
}
