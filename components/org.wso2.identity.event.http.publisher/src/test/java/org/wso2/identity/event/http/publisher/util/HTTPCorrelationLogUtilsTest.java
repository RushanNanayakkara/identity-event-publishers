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

package org.wso2.identity.event.http.publisher.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.methods.HttpPost;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import org.wso2.identity.event.http.publisher.internal.util.HTTPCorrelationLogUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.wso2.identity.event.http.publisher.internal.util.HTTPCorrelationLogUtils.triggerCorrelationLogForResponse;

/**
 * Test class for HTTPCorrelationLogUtils.
 */
public class HTTPCorrelationLogUtilsTest {

    @Test
    public void testHandleResponseCorrelationLog() {

        HttpPost mockRequest = mock(HttpPost.class);
        when(mockRequest.getFirstHeader("X-Correlation-ID")).thenReturn(
                new org.apache.http.message.BasicHeader("X-Correlation-ID", "test-correlation-id"));

        HTTPCorrelationLogUtils.handleResponseCorrelationLog(
                mockRequest, System.currentTimeMillis(), "completed", "200", "OK");
    }

    @Test
    public void testTriggerCorrelationLogForResponse() throws Exception {

        HttpPost mockRequest = mock(HttpPost.class);
        when(mockRequest.getMethod()).thenReturn("POST");
        when(mockRequest.getURI()).thenReturn(URI.create("http://localhost/test?param=value"));

        // Enable correlation logs for this test
        System.setProperty("enableCorrelationLogs", "true");

        // Reset the cached static field so the property is re-read
        Field enabledField = HTTPCorrelationLogUtils.class.getDeclaredField("isEnableCorrelationLogs");
        enabledField.setAccessible(true);
        enabledField.set(null, null);

        try (MockedStatic<LogFactory> mockedLogFactory = Mockito.mockStatic(LogFactory.class)) {
            Log mockLog = mock(Log.class);
            when(mockLog.isInfoEnabled()).thenReturn(true);
            mockedLogFactory.when(() -> LogFactory.getLog("correlation")).thenReturn(mockLog);

            // Remove final modifier and set the static correlationLog field to our mock
            Field logField = HTTPCorrelationLogUtils.class.getDeclaredField("correlationLog");
            logField.setAccessible(true);
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(logField, logField.getModifiers() & ~Modifier.FINAL);
            logField.set(null, mockLog);

            long startTime = System.currentTimeMillis() - 100;
            triggerCorrelationLogForResponse(mockRequest, startTime, "completed", "200", "OK");

            verify(mockLog, atLeastOnce()).info(anyString());
        }
    }
}
