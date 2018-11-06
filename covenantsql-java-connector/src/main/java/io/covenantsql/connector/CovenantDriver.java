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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

public class CovenantDriver implements Driver {
    public static final String PREFIX = "jdbc:covenantsql:";
    private static final Logger LOG = LoggerFactory.getLogger(CovenantDriver.class);
    private static int majorVersion = 0;
    private static int minorVersion = 0;

    static {
        // get version
        String version = CovenantDriver.class.getPackage().getImplementationVersion();
        try {
            majorVersion = Integer.valueOf(StringUtils.substringBefore(version, "."));
        } catch (NumberFormatException ignored) {
        }
        try {
            minorVersion = Integer.valueOf(StringUtils.substringAfterLast(version, "."));
        } catch (NumberFormatException ignored) {
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

    public static Connection createConnection(String url) throws SQLException {
        if (!isValidURL(url)) {
            throw new SQLException("invalid database address: " + url);
        }

        url = StringUtils.stripToEmpty(url);
        return new CovenantConnection(url);
    }

    /**
     * @param url
     * @param info
     * @return
     * @throws SQLException
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return createConnection(url);
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
        return majorVersion;
    }

    /**
     * @return
     */
    @Override
    public int getMinorVersion() {
        return minorVersion;
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
