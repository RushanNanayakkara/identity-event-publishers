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

package org.wso2.identity.event.websubhub.publisher.internal;

import org.apache.http.client.methods.HttpPost;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.identity.event.websubhub.publisher.config.WebSubAdapterConfiguration;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Test class for ClientManager.
 */
public class ClientManagerTest {

    private ClientManager clientManager;
    MockedStatic<WebSubHubAdapterDataHolder> mockedStaticDataHolder;

    @BeforeClass
    public void setUp() throws WebSubAdapterException {

        mockedStaticDataHolder = mockStatic(WebSubHubAdapterDataHolder.class);
        WebSubHubAdapterDataHolder mockDataHolder = mock(WebSubHubAdapterDataHolder.class);
        WebSubAdapterConfiguration mockConfiguration = mock(WebSubAdapterConfiguration.class);

        mockedStaticDataHolder.when(WebSubHubAdapterDataHolder::getInstance).thenReturn(mockDataHolder);

        when(mockDataHolder.getAdapterConfiguration()).thenReturn(mockConfiguration);
        when(mockConfiguration.getDefaultMaxConnections()).thenReturn(10);
        when(mockConfiguration.getDefaultMaxConnectionsPerRoute()).thenReturn(5);
        when(mockConfiguration.getHTTPConnectionTimeout()).thenReturn(3000);
        when(mockConfiguration.getHttpConnectionRequestTimeout()).thenReturn(3000);
        when(mockConfiguration.getHttpReadTimeout()).thenReturn(3000);
        when(mockConfiguration.isMtlsEnabled()).thenReturn(false);
        when(mockConfiguration.getExecutorCorePoolSize()).thenReturn(2);
        when(mockConfiguration.getExecutorMaxPoolSize()).thenReturn(4);
        when(mockConfiguration.getExecutorQueueCapacity()).thenReturn(10);

        clientManager = new ClientManager();
    }

    @Test
    public void testCreateHttpPost() throws WebSubAdapterException {

        TestPayload payload = new TestPayload("mockFieldValue");
        HttpPost post =
                clientManager.createHttpPost("http://mock-url.com", String.valueOf(payload), "test-correlation-id");
        Assert.assertNotNull(post);
        Assert.assertEquals(post.getMethod(), "POST");
    }

    @AfterClass
    public void tearDown() {

        if (mockedStaticDataHolder != null) {
            mockedStaticDataHolder.close();
        }
    }
}
