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

import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.carbon.identity.central.log.mgt.utils.LoggerUtils;
import org.wso2.carbon.utils.DiagnosticLog;
import org.wso2.identity.event.http.publisher.api.exception.HTTPAdapterClientException;
import org.wso2.identity.event.http.publisher.api.exception.HTTPAdapterServerException;
import org.wso2.identity.event.http.publisher.internal.constant.ErrorMessage;
import org.wso2.identity.event.http.publisher.internal.util.HTTPAdapterUtil;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit tests for HTTPAdapterUtil class.
 */
public class HTTPAdapterUtilTest {

    @Test
    public void testHandleClientException() {

        HTTPAdapterClientException exception = HTTPAdapterUtil.handleClientException(
                ErrorMessage.ERROR_PUBLISHING_EVENT_INVALID_PAYLOAD);
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.getMessage().contains("Invalid payload provided."));
    }

    @Test
    public void testHandleServerException() {

        HTTPAdapterServerException exception = HTTPAdapterUtil.handleServerException(
                ErrorMessage.ERROR_GETTING_ASYNC_CLIENT, new Exception("Test Exception"));
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.getMessage().contains("Error getting the async client to publish events."));
    }

    @Test
    public void testPrintPublisherDiagnosticLog() {

        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(true);
            mockedLoggerUtils.when(() -> LoggerUtils.triggerDiagnosticLogEvent(any())).then(invocation -> null);

            HTTPAdapterUtil.printPublisherDiagnosticLog(
                    "profileName",
                    "profileUri",
                    "event1,event2",
                    "http://endpoint",
                    "PUBLISH_EVENT",
                    DiagnosticLog.ResultStatus.SUCCESS,
                    "Published successfully"
            );

            mockedLoggerUtils.verify(() -> LoggerUtils.triggerDiagnosticLogEvent(any()));
        }
    }
}
