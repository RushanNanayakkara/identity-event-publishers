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

package org.wso2.identity.event.http.publisher.internal;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.identity.event.http.publisher.api.exception.HTTPAdapterException;
import org.wso2.identity.event.http.publisher.internal.component.ClientManager;
import org.wso2.identity.event.http.publisher.internal.component.HTTPAdapterDataHolder;
import org.wso2.identity.event.http.publisher.internal.config.HTTPAdapterConfiguration;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class for ClientManager.
 */
public class ClientManagerTest {

    private ClientManager clientManager;
    MockedStatic<HTTPAdapterDataHolder> mockedStaticDataHolder;

    @BeforeClass
    public void setUp() throws HTTPAdapterException {

        mockedStaticDataHolder = mockStatic(HTTPAdapterDataHolder.class);
        HTTPAdapterDataHolder mockDataHolder = mock(HTTPAdapterDataHolder.class);
        HTTPAdapterConfiguration mockConfiguration = mock(HTTPAdapterConfiguration.class);

        mockedStaticDataHolder.when(HTTPAdapterDataHolder::getInstance).thenReturn(mockDataHolder);

        when(mockDataHolder.getAdapterConfiguration()).thenReturn(mockConfiguration);
        when(mockConfiguration.getDefaultMaxConnections()).thenReturn(10);
        when(mockConfiguration.getDefaultMaxConnectionsPerRoute()).thenReturn(5);
        when(mockConfiguration.getHTTPConnectionTimeout()).thenReturn(3000);
        when(mockConfiguration.getHttpConnectionRequestTimeout()).thenReturn(3000);
        when(mockConfiguration.getHttpReadTimeout()).thenReturn(3000);
        when(mockConfiguration.getExecutorCorePoolSize()).thenReturn(2);
        when(mockConfiguration.getExecutorMaxPoolSize()).thenReturn(4);
        when(mockConfiguration.getExecutorQueueCapacity()).thenReturn(10);

        clientManager = new ClientManager();
    }

    @Test
    public void testCreateHttpPost() throws HTTPAdapterException {

        TestPayload payload = new TestPayload("mockFieldValue");
        String secret = "testSecret";
        HttpPost post = clientManager.createHttpPost("http://mock-url.com", payload.toString(), secret);
        Assert.assertNotNull(post);
        Assert.assertEquals(post.getMethod(), "POST");
        Assert.assertEquals(post.getURI().toString(), "http://mock-url.com");
    }

    @Test
    public void testExecuteAsyncCompleted() throws Exception {

        CloseableHttpAsyncClient mockAsyncClient = mock(CloseableHttpAsyncClient.class);
        ClientManager spyManager = spy(clientManager);
        doReturn(mockAsyncClient).when(spyManager).getHttpAsyncClient();

        HttpPost mockPost = mock(HttpPost.class);
        HttpResponse mockResponse = mock(HttpResponse.class);

        ArgumentCaptor<FutureCallback<HttpResponse>> callbackCaptor =
                ArgumentCaptor.forClass(FutureCallback.class);

        CompletableFuture<HttpResponse> future = spyManager.executeAsync(mockPost);

        verify(mockAsyncClient).execute(eq(mockPost), callbackCaptor.capture());
        callbackCaptor.getValue().completed(mockResponse);

        Assert.assertTrue(future.isDone());
        Assert.assertEquals(future.get(), mockResponse);
    }

    @Test
    public void testExecuteAsyncFailed() {

        CloseableHttpAsyncClient mockAsyncClient = mock(CloseableHttpAsyncClient.class);
        ClientManager spyManager = spy(clientManager);
        doReturn(mockAsyncClient).when(spyManager).getHttpAsyncClient();

        HttpPost mockPost = mock(HttpPost.class);

        ArgumentCaptor<FutureCallback<HttpResponse>> callbackCaptor =
                ArgumentCaptor.forClass(FutureCallback.class);

        CompletableFuture<HttpResponse> future = spyManager.executeAsync(mockPost);

        verify(mockAsyncClient).execute(eq(mockPost), callbackCaptor.capture());
        Exception ex = new Exception("fail");
        callbackCaptor.getValue().failed(ex);

        Assert.assertTrue(future.isCompletedExceptionally());
    }

    @Test
    public void testExecuteAsyncCancelled() {

        CloseableHttpAsyncClient mockAsyncClient = mock(CloseableHttpAsyncClient.class);
        ClientManager spyManager = spy(clientManager);
        doReturn(mockAsyncClient).when(spyManager).getHttpAsyncClient();

        HttpPost mockPost = mock(HttpPost.class);

        ArgumentCaptor<FutureCallback<HttpResponse>> callbackCaptor =
                ArgumentCaptor.forClass(FutureCallback.class);

        CompletableFuture<HttpResponse> future = spyManager.executeAsync(mockPost);

        verify(mockAsyncClient).execute(eq(mockPost), callbackCaptor.capture());
        callbackCaptor.getValue().cancelled();

        Assert.assertTrue(future.isCancelled());
    }

    @Test
    public void testGetAsyncCallbackExecutor() {

        Executor executor = clientManager.getAsyncCallbackExecutor();
        Assert.assertNotNull(executor, "Async callback executor should not be null");
    }

    @Test
    public void testGetMaxRetries() {

        HTTPAdapterConfiguration mockConfiguration = HTTPAdapterDataHolder.getInstance().getAdapterConfiguration();
        // Set up the mock to return a specific value
        when(mockConfiguration.getMaxRetries()).thenReturn(3);
        int maxRetries = clientManager.getMaxRetries();
        Assert.assertEquals(maxRetries, 3, "Max retries should match the configured value");
    }

    @Test
    public void testGetHttpAsyncClientStartsIfNotRunning() throws Exception {
        // Create a spy of ClientManager to inject a mock CloseableHttpAsyncClient
        ClientManager spyManager = spy(clientManager);
        CloseableHttpAsyncClient mockAsyncClient = mock(CloseableHttpAsyncClient.class);

        // Inject the mock into the private field using reflection
        Field field = ClientManager.class.getDeclaredField("httpAsyncClient");
        field.setAccessible(true);
        field.set(spyManager, mockAsyncClient);

        // Case 1: Not running, should call start()
        when(mockAsyncClient.isRunning()).thenReturn(false);
        spyManager.getHttpAsyncClient();
        verify(mockAsyncClient).start();

        // Case 2: Already running, should NOT call start() again
        when(mockAsyncClient.isRunning()).thenReturn(true);
        spyManager.getHttpAsyncClient();
        // start() should still only be called once
        verify(mockAsyncClient).start();
    }

    @AfterClass
    public void tearDown() {

        if (mockedStaticDataHolder != null) {
            mockedStaticDataHolder.close();
        }
    }

    // Simple payload class for testing
    static class TestPayload {

        private String field;

        public TestPayload(String field) {

            this.field = field;
        }

        public String getField() {

            return field;
        }

        public void setField(String field) {

            this.field = field;
        }
    }
}
