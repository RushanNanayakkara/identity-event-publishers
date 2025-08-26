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

package org.wso2.identity.event.http.publisher.internal.component;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.ssl.SSLContexts;
import org.wso2.carbon.identity.base.IdentityRuntimeException;
import org.wso2.identity.event.http.publisher.api.exception.HTTPAdapterException;
import org.wso2.identity.event.http.publisher.internal.util.HTTPAdapterUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;

import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.wso2.identity.event.http.publisher.internal.constant.ErrorMessage.ERROR_CREATING_HMAC_SIGNATURE;
import static org.wso2.identity.event.http.publisher.internal.constant.ErrorMessage.ERROR_CREATING_SSL_CONTEXT;
import static org.wso2.identity.event.http.publisher.internal.constant.ErrorMessage.ERROR_GETTING_ASYNC_CLIENT;
import static org.wso2.identity.event.http.publisher.internal.constant.ErrorMessage.ERROR_PUBLISHING_EVENT_INVALID_PAYLOAD;
import static org.wso2.identity.event.http.publisher.internal.constant.HTTPAdapterConstants.Http.HMAC_SHA256_ALGORITHM;
import static org.wso2.identity.event.http.publisher.internal.constant.HTTPAdapterConstants.Http.X_WSO2_EVENT_SIGNATURE;

/**
 * Class to retrieve the HTTP Clients.
 */
public class ClientManager {

    private static final Log LOG = LogFactory.getLog(ClientManager.class);
    private final CloseableHttpAsyncClient httpAsyncClient;
    /**
     * Global executor used for asynchronous callbacks.
     */
    private final Executor asyncCallbackExecutor;

    public ClientManager() throws HTTPAdapterException {

        try {
            int maxConnections =
                    HTTPAdapterDataHolder.getInstance().getAdapterConfiguration().getDefaultMaxConnections();
            int maxConnectionsPerRoute =
                    HTTPAdapterDataHolder.getInstance().getAdapterConfiguration().getDefaultMaxConnectionsPerRoute();

            IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                    .setConnectTimeout(HTTPAdapterDataHolder.getInstance().getAdapterConfiguration()
                            .getHTTPConnectionTimeout())
                    .setSoTimeout(
                            HTTPAdapterDataHolder.getInstance().getAdapterConfiguration().getHttpReadTimeout())
                    .setIoThreadCount(HTTPAdapterDataHolder.getInstance().getAdapterConfiguration().getIoThreadCount())
                    .build();
            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
            PoolingNHttpClientConnectionManager asyncConnectionManager =
                    new PoolingNHttpClientConnectionManager(ioReactor);
            asyncConnectionManager.setMaxTotal(maxConnections);
            asyncConnectionManager.setDefaultMaxPerRoute(maxConnectionsPerRoute);
            LOG.debug("PoolingNHttpClientConnectionManager created with maxConnections: " + maxConnections +
                    " and maxConnectionsPerRoute: " + maxConnectionsPerRoute);
            RequestConfig config = createRequestConfig();

            // Initialize HttpAsyncClient
            HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClients.custom()
                    .setDefaultRequestConfig(config)
                    .setConnectionManager(asyncConnectionManager)
                    .setSSLContext(createSSLContext());
            httpAsyncClient = httpAsyncClientBuilder.build();
            httpAsyncClient.start();
            LOG.debug("HttpAsyncClient started with config: connectTimeout=" +
                    config.getConnectTimeout() + ", connectionRequestTimeout=" +
                    config.getConnectionRequestTimeout() + ", socketTimeout=" +
                    config.getSocketTimeout() + ", maxConnections=" +
                    asyncConnectionManager.getMaxTotal() + ", maxConnectionsPerRoute=" +
                    asyncConnectionManager.getDefaultMaxPerRoute());

            // Custom handler that logs when the queue is full and discards the task.
            RejectedExecutionHandler handler = (r, executor) -> {
                LOG.warn("Async callback queue is full; discarding task of publishing events.");
                // the task is silently dropped
            };

            this.asyncCallbackExecutor = new ThreadPoolExecutor(
                    HTTPAdapterDataHolder.getInstance().getAdapterConfiguration().getExecutorCorePoolSize(),
                    HTTPAdapterDataHolder.getInstance().getAdapterConfiguration().getExecutorMaxPoolSize(),
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(HTTPAdapterDataHolder.getInstance().getAdapterConfiguration()
                            .getExecutorQueueCapacity()),
                    Executors.defaultThreadFactory(),
                    handler);
        } catch (IOException e) {
            throw HTTPAdapterUtil.handleServerException(ERROR_GETTING_ASYNC_CLIENT, e);
        }
    }

    /**
     * Get the executor for asynchronous callbacks.
     *
     * @return Executor instance for async callbacks.
     */
    public Executor getAsyncCallbackExecutor() {

        return asyncCallbackExecutor;
    }

    /**
     * Get the Max Retries for HTTP requests.
     *
     * @return Maximum number of retries.
     */
    public int getMaxRetries() {

        return HTTPAdapterDataHolder.getInstance().getAdapterConfiguration().getMaxRetries();
    }

    public CloseableHttpAsyncClient getHttpAsyncClient() {

        if (!httpAsyncClient.isRunning()) {
            LOG.debug("HttpAsyncClient is not running, starting client");
            httpAsyncClient.start();
        }
        return httpAsyncClient;
    }

    private RequestConfig createRequestConfig() {

        return RequestConfig.custom()
                .setConnectTimeout(HTTPAdapterDataHolder.getInstance().getAdapterConfiguration()
                        .getHTTPConnectionTimeout())
                .setConnectionRequestTimeout(HTTPAdapterDataHolder.getInstance().getAdapterConfiguration()
                        .getHttpConnectionRequestTimeout())
                .setSocketTimeout(
                        HTTPAdapterDataHolder.getInstance().getAdapterConfiguration().getHttpReadTimeout())
                .setRedirectsEnabled(false)
                .setRelativeRedirectsAllowed(false)
                .build();
    }

    private SSLContext createSSLContext() throws HTTPAdapterException {

        try {
            return SSLContexts.custom().build();
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw HTTPAdapterUtil.handleServerException(
                    ERROR_CREATING_SSL_CONTEXT, e);
        }
    }

    /**
     * Create an HTTP POST request.
     *
     * @param url     The URL for the HTTP POST request.
     * @param payload The payload to include in the request body.
     * @return A configured HttpPost instance.
     * @throws HTTPAdapterException If an error occurs while creating the request.
     */
    public HttpPost createHttpPost(String url, Object payload, String secret) throws HTTPAdapterException {

        HttpPost request = new HttpPost(url);
        request.setHeader(ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
        request.setHeader(CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        String jsonString;
        try {
            jsonString = mapper.writeValueAsString(payload);
            request.setEntity(new StringEntity(jsonString));
        } catch (IOException e) {
            throw HTTPAdapterUtil.handleClientException(ERROR_PUBLISHING_EVENT_INVALID_PAYLOAD);
        }

        // Add HMAC signature header if secret is provided
        if (secret != null && !secret.isEmpty()) {
            try {
                String signature = "sha256=" + hmacSha256Hex(secret, jsonString);
                request.setHeader(X_WSO2_EVENT_SIGNATURE, signature);
            } catch (Exception e) {
                throw HTTPAdapterUtil.handleClientException(ERROR_CREATING_HMAC_SIGNATURE);
            }
        }

        return request;
    }

    // Utility method for HMAC SHA-256 hex encoding
    private static String hmacSha256Hex(String secretKey, String data) throws NoSuchAlgorithmException,
            InvalidKeyException {

        Mac mac = Mac.getInstance(HMAC_SHA256_ALGORITHM);
        SecretKeySpec key =
                new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8),
                        HMAC_SHA256_ALGORITHM);
        mac.init(key);
        byte[] rawHmac = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : rawHmac) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * Execute an HTTP POST request asynchronously.
     *
     * @param httpPost The HTTP POST request to execute.
     * @return A CompletableFuture containing the HTTP response.
     */

    public CompletableFuture<HttpResponse> executeAsync(HttpPost httpPost) {

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();

        getHttpAsyncClient().execute(httpPost, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse result) {

                future.complete(result);
            }

            @Override
            public void failed(Exception ex) {

                future.completeExceptionally(
                        new IdentityRuntimeException(
                                "HTTP publisher async http client execution failed for URL: " + httpPost.getURI(),
                                ex
                        )
                );
            }

            @Override
            public void cancelled() {

                future.cancel(true);
            }
        });

        return future;
    }
}
