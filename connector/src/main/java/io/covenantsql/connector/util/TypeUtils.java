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

package io.covenantsql.connector.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

public class TypeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TypeUtils.class);

    public static int toSQLType(String type) {
        type = type.toUpperCase();

        if (StringUtils.contains(type, "INT")) {
            // integer type
            return Types.BIGINT;
        } else if (StringUtils.containsAny(type, "CHAR", "CLOB", "TEXT")) {
            return Types.VARCHAR;
        } else if (StringUtils.contains(type, "BLOB") || StringUtils.isEmpty(type)) {
            return Types.BLOB;
        } else if (StringUtils.containsAny(type, "REAL", "FLOA", "DOUB")) {
            return Types.DOUBLE;
        } else if (StringUtils.contains(type, "BOOLEAN")) {
            return Types.BOOLEAN;
        } else if (StringUtils.containsAny(type, "TIMESTAMP", "DATETIME")) {
            return Types.TIMESTAMP;
        } else if (StringUtils.contains(type, "TIME")) {
            return Types.TIME;
        } else if (StringUtils.contains(type, "DATE")) {
            return Types.DATE;
        } else if (StringUtils.contains(type, "DECIMAL")) {
            return Types.DECIMAL;
        } else {
            return Types.OTHER;
        }
    }

    public static int toSQLTypeWithDetection(String type, Object value) {
        int typeResult = toSQLType(type);

        if (typeResult != Types.OTHER) {
            return typeResult;
        }

        // detect variable type by object
        if (value instanceof String) {
            // maybe blob
            return Types.BLOB;
        } else if (value instanceof Number) {
            if (value instanceof Integer) {
                return Types.INTEGER;
            } else if (value instanceof Long) {
                return Types.BIGINT;
            } else if (value instanceof Float) {
                return Types.FLOAT;
            } else if (value instanceof Double) {
                return Types.DOUBLE;
            } else if (value instanceof BigInteger) {
                return Types.VARCHAR;
            } else if (value instanceof BigDecimal) {
                return Types.DECIMAL;
            }

            return Types.NUMERIC;
        } else {
            return Types.OTHER;
        }
    }

    public static Class toClass(int sqlType) throws SQLException {
        switch (sqlType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Boolean.class;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return Integer.class;
            case Types.BIGINT:
                return Long.class;
            case Types.DOUBLE:
                return Double.class;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BigDecimal.class;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BLOB:
            case Types.OTHER:
                return String.class;
            case Types.FLOAT:
            case Types.REAL:
                return Float.class;
            case Types.DATE:
                return java.sql.Date.class;
            case Types.TIMESTAMP:
                return Timestamp.class;
            case Types.TIME:
                return Time.class;
            default:
                throw new UnsupportedOperationException("Sql type " + sqlType + "is not supported");
        }
    }
}
