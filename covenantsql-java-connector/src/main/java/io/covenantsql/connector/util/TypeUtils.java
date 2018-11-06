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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

public class TypeUtils {
    public static int toSQLType(String type) {
        return Types.VARCHAR;
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
