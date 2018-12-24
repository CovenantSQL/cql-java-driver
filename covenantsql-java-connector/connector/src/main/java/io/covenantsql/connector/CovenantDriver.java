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
import io.covenantsql.connector.util.LogProxy;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

public class CovenantDriver implements Driver {
    public static final String PREFIX = "jdbc:covenantsql:";
    public static final String Version;
    public static final int MajorVersion;
    public static final int MinorVersion;

    private static final Logger LOG = LoggerFactory.getLogger(CovenantDriver.class);

    static {
        // get version
        Version = CovenantDriver.class.getPackage().getImplementationVersion();
        int majorVersion = 0;

        try {
            majorVersion = Integer.valueOf(StringUtils.substringBefore(Version, "."));
        } catch (NumberFormatException ignored) {
        } finally {
            MajorVersion = majorVersion;
        }

        int minorVersion = 0;

        try {
            minorVersion = Integer.valueOf(StringUtils.substringAfterLast(Version, "."));
        } catch (NumberFormatException ignored) {
        } finally {
            MinorVersion = minorVersion;
        }

        // register driver
        try {
            DriverManager.registerDriver(new CovenantDriver());
        } catch (SQLException e) {
            LOG.error("register covenantsql driver failed", e);
        }

        LOG.info("covenantsql driver registered");
    }

    /**
     * @param url
     * @return
     */
    public static boolean isValidURL(String url) {
        return url != null && StringUtils.startsWithIgnoreCase(url, PREFIX);
    }

    /**
     * @param url
     * @param info
     * @return
     * @throws SQLException
     */
    @Override
    public CovenantConnection connect(String url, Properties info) throws SQLException {
        return connect(url, new CovenantProperties(info));
    }

    public CovenantConnection connect(String url, CovenantProperties properties) throws SQLException {
        if (!isValidURL(url)) {
            throw new SQLException("invalid database address: " + url);
        }

        url = StringUtils.stripToEmpty(url);
        CovenantConnectionImpl connection = new CovenantConnectionImpl(url, properties);
        return LogProxy.wrap(CovenantConnection.class, connection);
    }

    /**
     * @param url
     * @return
     * @throws SQLException
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return isValidURL(url);
    }

    /**
     * @param url
     * @param info
     * @return
     * @throws SQLException
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    /**
     * @return
     */
    @Override
    public int getMajorVersion() {
        return MajorVersion;
    }

    /**
     * @return
     */
    @Override
    public int getMinorVersion() {
        return MinorVersion;
    }

    /**
     * @return
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /**
     * @return
     * @throws SQLFeatureNotSupportedException
     */
    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // not supported
        throw new SQLFeatureNotSupportedException();
    }
}
