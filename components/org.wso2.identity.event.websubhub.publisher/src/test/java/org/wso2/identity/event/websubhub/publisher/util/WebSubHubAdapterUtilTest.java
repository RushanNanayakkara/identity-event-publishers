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

import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterClientException;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterServerException;

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
    }

    @Test
    public void testHandleClientException() {

        WebSubAdapterClientException exception = WebSubHubAdapterUtil.handleClientException(
                WebSubHubAdapterConstants.ErrorMessages.ERROR_NULL_EVENT_PAYLOAD);
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.getMessage().contains("Invalid event payload input"));
    }

    @Test
    public void testHandleServerException() {

        WebSubAdapterServerException exception = WebSubHubAdapterUtil.handleServerException(
                WebSubHubAdapterConstants.ErrorMessages.ERROR_CREATING_ASYNC_HTTP_CLIENT,
                new Exception("Test Exception"));
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.getMessage().contains("Error while creating the Async HTTP client."));
    }
}
