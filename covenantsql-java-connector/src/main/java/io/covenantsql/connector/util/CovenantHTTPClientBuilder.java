/*
 * Copyright 2018 The ThunderDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.covenantsql.connector.util;

import io.covenantsql.connector.settings.CovenantProperties;
import io.covenantsql.connector.util.ssl.NonValidatingTrustManager;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.CertificateException;

public class CovenantHTTPClientBuilder {
    private static CovenantProperties properties;

    public CovenantHTTPClientBuilder(CovenantProperties properties) {
        this.properties = properties;
    }

    public CloseableHttpClient buildClient() throws Exception {
        return HttpClientBuilder.create()
            .setConnectionManager(getConnectionManager())
            .setDefaultConnectionConfig(getConnectionConfig())
            .setDefaultRequestConfig(getRequestConfig())
            .disableContentCompression()
            .build();
    }

    private PoolingHttpClientConnectionManager getConnectionManager()
        throws CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {
        RegistryBuilder<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http", PlainConnectionSocketFactory.getSocketFactory());

        if (properties.isSsl()) {
            registry.register("https", new SSLConnectionSocketFactory(getSSLContext(), new NoopHostnameVerifier()));
        }

        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(registry.build());
        return connectionManager;
    }

    private ConnectionConfig getConnectionConfig() {
        return ConnectionConfig.custom().build();
    }

    private RequestConfig getRequestConfig() {
        return RequestConfig.custom()
            .setConnectTimeout(properties.getConnectionTimeout())
            .build();
    }

    private SSLContext getSSLContext()
        throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
        TrustManager[] tms;

        switch (properties.getSslMode()) {
            case "none":
                tms = new TrustManager[]{new NonValidatingTrustManager()};
                break;
            case "strict":
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(getKeyStore());
                tms = tmf.getTrustManagers();
                break;
            default:
                throw new IllegalArgumentException("unknown ssl mode'" + properties.getSslMode() + "'");
        }

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(new KeyManager[]{}, tms, new SecureRandom());

        return ctx;
    }

    private KeyStore getKeyStore()
        throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException {
        KeyStore ks;
        InputStream keyStoreInputStream = new FileInputStream(properties.getKeyStorePath());

        try {
            ks = KeyStore.getInstance("PKCS12");
            ks.load(keyStoreInputStream, properties.getKeyStorePassword().toCharArray());
        } catch (KeyStoreException e) {
            throw new NoSuchAlgorithmException("PKCS12 KeyStore is not available");
        }

        return ks;
    }
}
