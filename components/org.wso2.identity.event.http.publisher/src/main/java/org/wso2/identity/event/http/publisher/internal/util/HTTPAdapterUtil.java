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

package org.wso2.identity.event.http.publisher.internal.util;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.identity.central.log.mgt.utils.LoggerUtils;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.carbon.utils.DiagnosticLog;
import org.wso2.identity.event.http.publisher.api.exception.HTTPAdapterClientException;
import org.wso2.identity.event.http.publisher.api.exception.HTTPAdapterServerException;
import org.wso2.identity.event.http.publisher.internal.constant.ErrorMessage;
import org.wso2.identity.event.http.publisher.internal.constant.HTTPAdapterConstants;

/**
 * Utility class for HTTPAdapter.
 */
public class HTTPAdapterUtil {

    private static final Log log = LogFactory.getLog(HTTPAdapterUtil.class);

    private HTTPAdapterUtil() {

    }

    /**
     * Get the correlation ID.
     *
     * @return Correlation ID.
     */
    public static String getCorrelationID(SecurityEventTokenPayload eventTokenPayload) {

        return eventTokenPayload.getRci();
    }

    /**
     * Handle client exceptions.
     *
     * @param error Error message.
     * @param data  Data.
     * @return HTTPAdapterClientException.
     */
    public static HTTPAdapterClientException handleClientException(ErrorMessage error, String... data) {

        String description = error.getDescription();
        if (ArrayUtils.isNotEmpty(data)) {
            description = String.format(description, data);
        }
        return new HTTPAdapterClientException(error.getMessage(), description, error.getCode());
    }

    /**
     * Handle server exceptions.
     *
     * @param error     Error message.
     * @param throwable Throwable.
     * @param data      Data.
     * @return HTTPAdapterServerException.
     */
    public static HTTPAdapterServerException handleServerException(ErrorMessage error, Throwable throwable,
                                                                   String... data) {

        String description = error.getDescription();
        if (ArrayUtils.isNotEmpty(data)) {
            description = String.format(description, data);
        }
        return new HTTPAdapterServerException(error.getMessage(), description, error.getCode(), throwable);
    }

    /**
     * Print diagnostic log for publisher operations.
     *
     * @param eventProfileName Name of the event profile.
     * @param eventProfileUri  URI of the event profile.
     * @param events           Events published.
     * @param endpoint         Endpoint URL.
     * @param action           Action performed.
     * @param status           Result status.
     * @param message          Result message.
     */
    public static void printPublisherDiagnosticLog(String eventProfileName, String eventProfileUri, String events,
                                                   String endpoint, String action, DiagnosticLog.ResultStatus status,
                                                   String message) {

        if (LoggerUtils.isDiagnosticLogsEnabled()) {
            DiagnosticLog.DiagnosticLogBuilder diagnosticLogBuilder = new DiagnosticLog.DiagnosticLogBuilder(
                    HTTPAdapterConstants.LogConstants.HTTP_ADAPTER, action);
            diagnosticLogBuilder
                    .inputParam(HTTPAdapterConstants.LogConstants.InputKeys.ENDPOINT, endpoint)
                    .inputParam(HTTPAdapterConstants.LogConstants.InputKeys.EVENT_URI, eventProfileUri)
                    .inputParam(HTTPAdapterConstants.LogConstants.InputKeys.EVENT_PROFILE_NAME,
                            eventProfileName)
                    .inputParam(HTTPAdapterConstants.LogConstants.InputKeys.EVENTS, events)
                    .resultMessage(message)
                    .resultStatus(status)
                    .logDetailLevel(DiagnosticLog.LogDetailLevel.APPLICATION);
            LoggerUtils.triggerDiagnosticLogEvent(diagnosticLogBuilder);
        }
    }
}
