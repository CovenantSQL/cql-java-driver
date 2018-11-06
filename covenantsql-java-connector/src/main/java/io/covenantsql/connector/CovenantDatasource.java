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

import io.covenantsql.connector.settings.CovenantProperties;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class CovenantDatasource implements DataSource {
    private final CovenantDriver driver = new CovenantDriver();
    private final String url;
    private int loginTimeout;
    private PrintWriter printWriter;
    private CovenantProperties properties;

    public CovenantDatasource(String url) {
        this(url, new CovenantProperties());
    }

    public CovenantDatasource(String url, Properties info) {
        this(url, new CovenantProperties(info));
    }

    public CovenantDatasource(String url, CovenantProperties properties) {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect CovenantSQL jdbc url. It must be not null");
        }

        this.url = url;

        try {
            this.properties = CovenantURLParser.parse(url, properties.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public CovenantConnection getConnection() throws SQLException {
        return driver.connect(url, properties);
    }

    @Override
    public CovenantConnection getConnection(String username, String password) throws SQLException {
        // no username/password is required
        return driver.connect(url, properties);
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

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.printWriter = out;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeout = seconds;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
