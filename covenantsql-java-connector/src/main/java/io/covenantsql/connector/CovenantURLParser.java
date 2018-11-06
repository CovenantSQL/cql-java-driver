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

import io.covenantsql.connector.settings.CovenantConnectionSettings;
import io.covenantsql.connector.settings.CovenantProperties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CovenantURLParser {
    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBC_DB_PREFIX = JDBC_PREFIX + "covenantsql:";
    public static final Pattern DB_PATTERN = Pattern.compile("/([a-zA-Z0-9])+/");
    private static final Logger LOG = LoggerFactory.getLogger(CovenantURLParser.class);

    private CovenantURLParser() {
    }

    public static CovenantProperties parse(String url, Properties defaults) throws URISyntaxException {
        if (!url.startsWith(JDBC_DB_PREFIX)) {
            throw new URISyntaxException(url, "'" + JDBC_DB_PREFIX + "' prefix is mandatory");
        }

        return parseURL(StringUtils.substringAfter(url, JDBC_PREFIX), defaults);
    }

    private static CovenantProperties parseURL(String uriString, Properties defaults) throws URISyntaxException {
        URI uri = new URI(uriString);
        Properties urlProperties = parseURIQueryPart(uri, defaults);
        CovenantProperties props = new CovenantProperties(urlProperties);
        props.setHost(uri.getHost());
        int port = uri.getPort();
        if (port == -1) {
            throw new IllegalArgumentException("port is missed or wrong");
        }
        props.setPort(port);
        String database = uri.getPath();
        if (StringUtils.isEmpty(database)) {
            database = defaults.getProperty(CovenantConnectionSettings.DATABASE.getKey());
        }

        database = StringUtils.stripToEmpty(database);

        Matcher m = DB_PATTERN.matcher(database);

        if (m.matches()) {
            database = m.group(1);
        } else {
            throw new URISyntaxException("wrong database path: '" + database + "'", uriString);
        }

        props.setDatabase(database);
        return props;
    }

    private static Properties parseURIQueryPart(URI uri, Properties defaults) {
        String query = uri.getQuery();
        if (query == null) {
            return defaults;
        }

        Properties urlProps = new Properties(defaults);
        String queryKeyValues[] = query.split("&");
        for (String keyValue : queryKeyValues) {
            String keyValueTokens[] = keyValue.split("=");
            if (keyValueTokens.length == 2) {
                urlProps.put(keyValueTokens[0], keyValueTokens[1]);
            } else {
                LOG.warn("don't know how to handle parameter pair: {}", keyValue);
            }
        }
        return urlProps;
    }
}
