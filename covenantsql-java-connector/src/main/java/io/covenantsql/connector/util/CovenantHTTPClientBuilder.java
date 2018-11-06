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
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;

public class CovenantHTTPClientBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(CovenantHTTPClientBuilder.class);
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
        throws CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException, UnrecoverableKeyException {
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
        throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException, UnrecoverableKeyException {
        SSLContextBuilder ctxBuilder = SSLContexts.custom().loadKeyMaterial(getKeyStore(), "".toCharArray());

        if (StringUtils.equalsIgnoreCase(properties.getSslMode(), "none")) {
            ctxBuilder.loadTrustMaterial(new TrustAllStrategy());
        }

        return ctxBuilder.build();
    }

    private KeyStore getKeyStore()
        throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, null);

        // load private key
        InputStream keyStream = getFileStream(properties.getKeyPath());
        PEMParser keyParser = new PEMParser(new InputStreamReader(keyStream));
        JcaPEMKeyConverter keyConverter = new JcaPEMKeyConverter();
        PrivateKey key = keyConverter.getKeyPair((PEMKeyPair) keyParser.readObject()).getPrivate();

        // load certificate
        InputStream certStream = getFileStream(properties.getCertPath());
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        Certificate cert = certFactory.generateCertificate(certStream);

        // set to keystore
        ks.setKeyEntry("client", key, "".toCharArray(), new Certificate[]{cert});

        return ks;
    }

    private InputStream getFileStream(String fileName) throws IOException {
        InputStream stream = null;

        try {
            stream = new FileInputStream(fileName);
        } catch (FileNotFoundException fe) {
            // try get file from resources
            stream = getClass().getResourceAsStream("/" + fileName);
        }

        if (stream == null) {
            throw new IOException("load key/cert file " + fileName + " failed");
        }

        return stream;
    }
}
