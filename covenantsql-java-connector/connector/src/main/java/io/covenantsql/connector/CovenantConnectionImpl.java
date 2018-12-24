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

package io.covenantsql.connector;

import io.covenantsql.connector.except.CovenantException;
import io.covenantsql.connector.settings.CovenantProperties;
import io.covenantsql.connector.util.CovenantHTTPClientBuilder;
import io.covenantsql.connector.util.LogProxy;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class CovenantConnectionImpl extends CovenantMockConnectionUnused implements CovenantConnection {
    private static final Logger LOG = LoggerFactory.getLogger(CovenantConnectionImpl.class);
    private final CloseableHttpClient httpClient;
    private final CovenantProperties properties;
    private String url;
    private boolean closed = false;

    public CovenantConnectionImpl(String url) {
        this(url, new CovenantProperties());
    }

    public CovenantConnectionImpl(String url, CovenantProperties properties) {
        this.url = url;
        try {
            this.properties = CovenantURLParser.parse(url, properties.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        CovenantHTTPClientBuilder clientBuilder = new CovenantHTTPClientBuilder(this.properties);
        LOG.debug("new connection");
        try {
            httpClient = clientBuilder.buildClient();
        } catch (Exception e) {
            throw new IllegalStateException("cannot initialize http client", e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            httpClient.close();
            closed = true;
        } catch (IOException e) {
            throw new CovenantException("HTTP client close exception", e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return LogProxy.wrap(DatabaseMetaData.class, new CovenantDatabaseMetadata(url, this));
    }


    @Override
    public String getCatalog() throws SQLException {
        return properties.getDatabase();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        properties.setDatabase(catalog);
        URI old = URI.create(StringUtils.substringAfter(url, CovenantURLParser.JDBC_PREFIX));
        try {
            url = CovenantURLParser.JDBC_PREFIX +
                new URI(old.getScheme(), old.getUserInfo(), old.getHost(), old.getPort(),
                    "/" + catalog, old.getQuery(), old.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public CovenantStatement createStatement() throws SQLException {
        return LogProxy.wrap(CovenantStatement.class, new CovenantStatementImpl(httpClient, this, properties));
    }

    @Override
    public CovenantStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public CovenantStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY && resultSetConcurrency != ResultSet.CONCUR_READ_ONLY
            && resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException();
        }
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return createPreparedStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return createPreparedStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    public PreparedStatement createPreparedStatement(String sql) throws SQLException {
        return LogProxy.wrap(PreparedStatement.class, new CovenantPreparedStatementImpl(httpClient, this, properties, sql));
    }

    private CovenantStatement createCovenantStatemnt(CloseableHttpClient httpClient) throws SQLException {
        return LogProxy.wrap(CovenantStatement.class, new CovenantStatementImpl(httpClient, this, properties));
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (isClosed()) {
            return false;
        }

        try {
            CovenantProperties properties = new CovenantProperties(this.properties);
            properties.setConnectionTimeout((int) TimeUnit.SECONDS.toMillis(timeout));
            CloseableHttpClient client = new CovenantHTTPClientBuilder(properties).buildClient();
            Statement statement = createCovenantStatemnt(client);
            statement.execute("SELECT 1");
            statement.close();
            return true;
        } catch (Exception e) {
            boolean isFailOnConnectionTimeout = e.getCause() instanceof ConnectTimeoutException;
            if (!isFailOnConnectionTimeout) {
                LOG.warn("ping covenantsql database failed", e);
            }
        }

        return false;
    }

    @Override
    public String getSchema() throws SQLException {
        return properties.getDatabase();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        properties.setDatabase(schema);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.close();
    }

    String getUrl() {
        return url;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
