/*
 * Copyright (c) 2024-2025, WSO2 LLC. (http://www.wso2.com).
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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.ssl.SSLContexts;
import org.wso2.carbon.core.RegistryResources;
import org.wso2.carbon.identity.base.IdentityRuntimeException;
import org.wso2.carbon.identity.core.IdentityKeyStoreResolver;
import org.wso2.carbon.identity.core.util.IdentityKeyStoreResolverException;
import org.wso2.carbon.identity.core.util.IdentityKeyStoreResolverUtil;
import org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterException;
import org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.wso2.carbon.base.MultitenantConstants.SUPER_TENANT_DOMAIN_NAME;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_PUBLISHING_EVENT_INVALID_PAYLOAD;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.CORRELATION_ID_REQUEST_HEADER;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.WEBSUBHUB_KEYSTORE_NAME;

/**
 * Class to retrieve the HTTP Clients.
 */
public class ClientManager {

    private static final Log LOG = LogFactory.getLog(ClientManager.class);
    private final CloseableHttpAsyncClient httpAsyncClient;
    private final CloseableHttpClient httpClient;
    private CloseableHttpClient mtlsHttpClient = null;
    /**
     * Global executor used for asynchronous callbacks.
     */
    private final Executor asyncCallbackExecutor;

    public ClientManager() throws WebSubAdapterException {

        try {
            PoolingNHttpClientConnectionManager asyncConnectionManager =
                    createPoolingConnectionManager(PoolingNHttpClientConnectionManager.class);
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

            // Initialize CloseableHttpClient
            PoolingHttpClientConnectionManager syncConnectionManager =
                    createPoolingConnectionManager(PoolingHttpClientConnectionManager.class);
            httpClient = HttpClients.custom()
                    .setDefaultRequestConfig(config)
                    .setConnectionManager(syncConnectionManager)
                    .setSSLContext(createSSLContext())
                    .build();
            LOG.debug("CloseableHttpClient initialized with config: connectTimeout=" +
                    config.getConnectTimeout() + ", connectionRequestTimeout=" +
                    config.getConnectionRequestTimeout() + ", socketTimeout=" +
                    config.getSocketTimeout() + ", maxConnections=" +
                    syncConnectionManager.getMaxTotal() + ", maxConnectionsPerRoute=" +
                    syncConnectionManager.getDefaultMaxPerRoute());

            // Initialize MTLS HttpClient
            if (WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration()
                    .isMtlsEnabled()) {
                mtlsHttpClient = getMTLSClient();
            }

            // Custom handler that logs when the queue is full and discards the task.
            RejectedExecutionHandler handler = (r, executor) -> {
                LOG.error(
                        "Async callback queue is full; discarding task of publishing events. " +
                                "Please attend immediately.");
                // the task is silently dropped
            };

            this.asyncCallbackExecutor = new ThreadPoolExecutor(
                    WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration().getExecutorCorePoolSize(),
                    WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration().getExecutorMaxPoolSize(),
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration()
                            .getExecutorQueueCapacity()),
                    Executors.defaultThreadFactory(),
                    handler);
        } catch (IOException e) {
            throw WebSubHubAdapterUtil.handleServerException(
                    WebSubHubAdapterConstants.ErrorMessages.ERROR_GETTING_ASYNC_CLIENT, e);
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

        return WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration().getMaxRetries();
    }

    private CloseableHttpClient getMTLSClient() throws WebSubAdapterException {

        try {
            IdentityKeyStoreResolver resolver = IdentityKeyStoreResolver.getInstance();

            // Load custom keystore and truststore
            KeyStore customKeyStore = resolver.getCustomKeyStore(SUPER_TENANT_DOMAIN_NAME, WEBSUBHUB_KEYSTORE_NAME);

            KeyStore trustStore = resolver.getTrustStore(SUPER_TENANT_DOMAIN_NAME);

            // Initialize KeyManagerFactory with custom keystore
            KeyManagerFactory keyManagerFactory =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(customKeyStore, resolver.getCustomKeyStoreConfig(
                    IdentityKeyStoreResolverUtil.buildCustomKeyStoreName(WEBSUBHUB_KEYSTORE_NAME),
                    RegistryResources.SecurityManagement.CustomKeyStore.PROP_KEY_PASSWORD).toCharArray());

            // Initialize TrustManagerFactory with truststore
            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            // Build SSLContext
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(),
                    new SecureRandom());

            // Disable hostname verification
            // TODO: Enable once the hostname verification is properly configured
            HostnameVerifier allowAllHosts = (hostname, session) -> {
                LOG.warn("Hostname verification disabled: accepted hostname = " + hostname);
                return true;
            };

            SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(
                    sslContext,
                    new String[] {"TLSv1.2"},
                    null,
                    allowAllHosts);

            return HttpClients.custom()
                    .setSSLSocketFactory(sslSocketFactory)
                    .disableConnectionState()
                    .disableCookieManagement()
                    .build();

        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException |
                 UnrecoverableKeyException | IdentityKeyStoreResolverException e) {
            LOG.error("Error while initializing MTLS client", e);
            throw WebSubHubAdapterUtil.handleServerException(
                    WebSubHubAdapterConstants.ErrorMessages.ERROR_CREATING_SSL_CONTEXT, e);
        }
    }

    public CloseableHttpAsyncClient getHttpAsyncClient() {

        if (!httpAsyncClient.isRunning()) {
            LOG.debug("HttpAsyncClient is not running, starting client");
            httpAsyncClient.start();
        }
        return httpAsyncClient;
    }

    public CloseableHttpClient getHttpClient() {

        return httpClient;
    }

    /**
     * Get the effective HTTP client based on the configuration.
     *
     * @return CloseableHttpClient instance.
     */
    public CloseableHttpClient getEffectiveHttpClient() {

        if (WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration()
                .isMtlsEnabled()) {
            return mtlsHttpClient;
        }
        return getHttpClient();
    }

    private RequestConfig createRequestConfig() {

        return RequestConfig.custom()
                .setConnectTimeout(WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration()
                        .getHTTPConnectionTimeout())
                .setConnectionRequestTimeout(WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration()
                        .getHttpConnectionRequestTimeout())
                .setSocketTimeout(
                        WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration().getHttpReadTimeout())
                .setRedirectsEnabled(false)
                .setRelativeRedirectsAllowed(false)
                .build();
    }

    private SSLContext createSSLContext() throws WebSubAdapterException {

        try {
            return SSLContexts.custom()
                    .loadTrustMaterial(WebSubHubAdapterDataHolder.getInstance().getTrustStore(), null)
                    .build();
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw WebSubHubAdapterUtil.handleServerException(
                    WebSubHubAdapterConstants.ErrorMessages.ERROR_CREATING_SSL_CONTEXT, e);
        }
    }

    private <T> T createPoolingConnectionManager(Class<T> managerType) throws IOException {

        int maxConnections =
                WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration().getDefaultMaxConnections();
        int maxConnectionsPerRoute =
                WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration().getDefaultMaxConnectionsPerRoute();

        if (managerType.equals(PoolingNHttpClientConnectionManager.class)) {
            IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                    .setConnectTimeout(WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration()
                            .getHTTPConnectionTimeout())
                    .setSoTimeout(
                            WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration().getHttpReadTimeout())
                    .setIoThreadCount(
                            WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration().getIoThreadCount())
                    .build();

            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
            PoolingNHttpClientConnectionManager manager = new PoolingNHttpClientConnectionManager(ioReactor);
            manager.setMaxTotal(maxConnections);
            manager.setDefaultMaxPerRoute(maxConnectionsPerRoute);
            LOG.debug("PoolingNHttpClientConnectionManager created with maxConnections: " + maxConnections +
                    " and maxConnectionsPerRoute: " + maxConnectionsPerRoute);
            return managerType.cast(manager);
        } else if (managerType.equals(PoolingHttpClientConnectionManager.class)) {
            PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
            manager.setMaxTotal(maxConnections);
            manager.setDefaultMaxPerRoute(maxConnectionsPerRoute);
            LOG.debug("PoolingHttpClientConnectionManager created with maxConnections: " + maxConnections +
                    " and maxConnectionsPerRoute: " + maxConnectionsPerRoute);
            return managerType.cast(manager);
        } else {
            throw new IllegalArgumentException("Unsupported connection manager type: " + managerType.getName());
        }
    }

    /**
     * Create an HTTP POST request.
     *
     * @param url           The URL for the HTTP POST request.
     * @param bodyJson      The JSON body for the request.
     * @param correlationId The correlation ID for tracing.
     * @return A configured HttpPost instance.
     * @throws WebSubAdapterException If an error occurs while creating the request.
     */
    public HttpPost createHttpPost(String url, String bodyJson, String correlationId)
            throws WebSubAdapterException {

        HttpPost request = new HttpPost(url);
        request.setHeader(ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
        request.setHeader(CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        if (StringUtils.isNotBlank(correlationId)) {
            request.setHeader(CORRELATION_ID_REQUEST_HEADER, correlationId);
        }
        try {
            if (bodyJson != null) {
                request.setEntity(new StringEntity(bodyJson));
            }
        } catch (UnsupportedEncodingException e) {
            throw WebSubHubAdapterUtil.handleClientException(ERROR_PUBLISHING_EVENT_INVALID_PAYLOAD);
        }
        return request;
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
                                "WebSubHub publisher async http client execution failed for URL: " + httpPost.getURI(),
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

    /**
     * Execute an HTTP POST request synchronously.
     *
     * @param httpPost The HTTP POST request to execute.
     * @return The HTTP response.
     */
    public HttpResponse execute(HttpPost httpPost) throws IOException {

        return getHttpClient().execute(httpPost);
    }

    /**
     * Execute an HTTP POST request for subscriber requests.
     *
     * @param httpPost The HTTP POST request to execute.
     * @return The HTTP response.
     * @throws IOException            If an I/O error occurs.
     * @throws WebSubAdapterException If an error occurs while executing the request.
     */
    public HttpResponse executeSubscriberRequest(HttpPost httpPost) throws IOException, WebSubAdapterException {

        return getEffectiveHttpClient().execute(httpPost);
    }
}
