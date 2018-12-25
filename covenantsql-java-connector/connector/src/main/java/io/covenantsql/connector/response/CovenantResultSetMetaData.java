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

package io.covenantsql.connector.response;

import io.covenantsql.connector.util.TypeUtils;

import java.sql.SQLException;

public class CovenantResultSetMetaData extends CovenantMockResultSetMetaDataUnused {
    private final CovenantResultSet resultSet;

    public CovenantResultSetMetaData(CovenantResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNoNulls;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 80;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return resultSet.getColumnNames().length;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return resultSet.getColumnNames()[column - 1];
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return resultSet.getColumnNames()[column - 1];
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return resultSet.getTable();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return resultSet.getDB();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return TypeUtils.toSQLType(getColumnTypeName(column));
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return resultSet.getTypes()[column - 1];
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        String columnTypeName = getColumnTypeName(column);
        int sqlType = TypeUtils.toSQLType(columnTypeName);
        return TypeUtils.toClass(sqlType).getName();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("Unable to unwrap to " + iface.toString());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }
}
