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

package io.covenantsql.connector.settings;

import java.util.Properties;

public class CovenantProperties {

    // connection settings
    private String host;
    private int port;

    private int connectionTimeout;
    private int operationTimeout;
    private String keyStorePath;
    private String keyStorePassword;
    private boolean ssl;
    private String sslMode;
    private String database;

    public CovenantProperties() {
        this(new Properties());
    }

    public CovenantProperties(Properties info) {
        this.connectionTimeout = getSetting(info, CovenantConnectionSettings.CONNECTION_TIMEOUT);
        this.operationTimeout = getSetting(info, CovenantConnectionSettings.OPERATION_TIMEOUT);
        this.keyStorePath = getSetting(info, CovenantConnectionSettings.KEYSTORE_PATH);
        this.keyStorePassword = getSetting(info, CovenantConnectionSettings.KEYSTORE_PASSWORD);
        this.ssl = getSetting(info, CovenantConnectionSettings.SSL);
        this.sslMode = getSetting(info, CovenantConnectionSettings.SSL_MODE);
        this.database = getSetting(info, CovenantConnectionSettings.DATABASE);
    }

    public CovenantProperties(CovenantProperties properties) {
        setHost(properties.host);
        setPort(properties.port);
        setConnectionTimeout(properties.connectionTimeout);
        setOperationTimeout(properties.operationTimeout);
        setKeyStorePath(properties.keyStorePath);
        setKeyStorePassword(properties.keyStorePassword);
        setSsl(properties.ssl);
        setSslMode(properties.sslMode);
        setDatabase(properties.database);
    }

    public Properties asProperties() {
        PropertiesBuilder ret = new PropertiesBuilder();
        ret.put(CovenantConnectionSettings.CONNECTION_TIMEOUT.getKey(), String.valueOf(connectionTimeout));
        ret.put(CovenantConnectionSettings.OPERATION_TIMEOUT.getKey(), String.valueOf(operationTimeout));
        ret.put(CovenantConnectionSettings.KEYSTORE_PATH.getKey(), String.valueOf(keyStorePath));
        ret.put(CovenantConnectionSettings.KEYSTORE_PASSWORD.getKey(), String.valueOf(keyStorePassword));
        ret.put(CovenantConnectionSettings.SSL.getKey(), String.valueOf(ssl));
        ret.put(CovenantConnectionSettings.SSL_MODE.getKey(), String.valueOf(sslMode));
        ret.put(CovenantConnectionSettings.DATABASE.getKey(), String.valueOf(database));

        return ret.getProperties();
    }

    private <T> T getSetting(Properties info, CovenantConnectionSettings settings) {
        return getSetting(info, settings.getKey(), settings.getDefaultValue(), settings.getClazz());
    }

    @SuppressWarnings("unchecked")
    private <T> T getSetting(Properties info, String key, Object defaultValue, Class clazz) {
        String val = info.getProperty(key);
        if (val == null) {
            return (T) defaultValue;
        }
        if (clazz == int.class || clazz == Integer.class) {
            return (T) clazz.cast(Integer.valueOf(val));
        }
        if (clazz == long.class || clazz == Long.class) {
            return (T) clazz.cast(Long.valueOf(val));
        }
        if (clazz == boolean.class || clazz == Boolean.class) {
            final Boolean boolValue;
            if ("1".equals(val) || "0".equals(val)) {
                boolValue = "1".equals(val);
            } else {
                boolValue = Boolean.valueOf(val);
            }
            return (T) clazz.cast(boolValue);
        }
        return (T) clazz.cast(val);
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getOperationTimeout() {
        return operationTimeout;
    }

    public void setOperationTimeout(int operationTimeout) {
        this.operationTimeout = operationTimeout;
    }

    public String getKeyStorePath() {
        return keyStorePath;
    }

    public void setKeyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public String getSslMode() {
        return sslMode;
    }

    public void setSslMode(String sslMode) {
        this.sslMode = sslMode;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    private static class PropertiesBuilder {
        private final Properties properties;

        public PropertiesBuilder() {
            properties = new Properties();
        }

        public void put(String key, int value) {
            properties.put(key, value);
        }

        public void put(String key, Integer value) {
            if (value != null) {
                properties.put(key, value.toString());
            }
        }

        public void put(String key, Long value) {
            if (value != null) {
                properties.put(key, value.toString());
            }
        }

        public void put(String key, boolean value) {
            properties.put(key, String.valueOf(value));
        }

        public void put(String key, String value) {
            if (value != null) {
                properties.put(key, value);
            }
        }

        public Properties getProperties() {
            return properties;
        }
    }
}
