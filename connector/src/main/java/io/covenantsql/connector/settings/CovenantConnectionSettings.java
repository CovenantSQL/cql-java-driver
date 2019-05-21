/*
 * Copyright 2018 The CovenantSQL Authors.
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

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public enum CovenantConnectionSettings {
    CONNECTION_TIMEOUT("connection_timeout", 10 * 1000, "connection timeout in milliseoncds"),
    OPERATION_TIMEOUT("operation_timeout", 60 * 1000, "operation timeout in milliseconds"),
    SSL("ssl", false, "enable SSL/TLS for the connection"),
    SSL_MODE("sslmode", "strict", "verify certificate or not: none (don't verify), strict (verify)"),
    KEY_PATH("key_path", "", "client key use for requests in ssl mode"),
    CERT_PATH("cert_path", "", "client certificate use for requests in ssl mode"),
    DATABASE("database", "", "database use to query");


    private final String key;
    private final Object defaultValue;
    private final String description;
    private final Class clazz;

    CovenantConnectionSettings(String key, Object defaultValue, String description) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = defaultValue.getClass();
        this.description = description;
    }

    public String getKey() {
        return key;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public Class getClazz() {
        return clazz;
    }

    public DriverPropertyInfo createDriverPropertyInfo(Properties properties) {
        DriverPropertyInfo propertyInfo = new DriverPropertyInfo(key, driverPropertyValue(properties));
        propertyInfo.required = false;
        propertyInfo.description = description;
        propertyInfo.choices = driverPropertyInfoChoices();
        return propertyInfo;
    }

    private String[] driverPropertyInfoChoices() {
        return clazz == Boolean.class || clazz == Boolean.TYPE ? new String[]{"true", "false"} : null;
    }

    private String driverPropertyValue(Properties properties) {
        String value = properties.getProperty(key);
        if (value == null) {
            value = defaultValue == null ? null : defaultValue.toString();
        }
        return value;
    }
}
