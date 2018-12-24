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

package io.covenantsql.connector.response;

import io.covenantsql.connector.CovenantStatement;
import io.covenantsql.connector.response.beans.CovenantQueryResponseBean;
import io.covenantsql.connector.util.TypeUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class CovenantResultSet extends CovenantMockResultSetUnused {
    public static final CovenantResultSet EMPTY;
    private static final String[] dateFormats = {
        "yyyy-MM-dd'T'HH:mm:ssXXX",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss",
        "MMMM/dd/yyyy HH:mm:ss",
        "MMMM dd, yyyy HH:mm:ss",
        "dd/MM/yyyy HH:mm:ss",
        "dd-MM-yyyy HH:mm:ss",
        "yyyy-MM-dd",
        "MMMM/dd/yyyy",
        "MMMM dd, yyyy",
        "dd/MM/yyyy",
        "dd-MM-yyyy",
    };

    static {
        EMPTY = new CovenantResultSet();
    }

    private final String db;
    private final String table;

    private final Map<String, Integer> col = new HashMap<>();
    private final String[] types;
    private final String[] columns;
    private final CovenantQueryResponseBean.CovenantQueryResponseDataBean dataBean;

    private int maxRows;
    private int lastReadColumn;
    private int rowNumber;
    private CovenantStatement statement;
    private Object[] values;
    private boolean closed = false;

    private CovenantResultSet() {
        this.db = "";
        this.table = "";
        this.types = new String[0];
        this.columns = new String[0];
        this.dataBean = null;
        this.closed = true;
    }

    public CovenantResultSet(CovenantQueryResponseBean.CovenantQueryResponseDataBean bean, String db, String table,
                             CovenantStatement statement) {
        this.db = db;
        this.table = table;
        this.statement = statement;
        this.dataBean = bean;
        this.types = bean.getTypes().toArray(new String[0]);
        this.columns = bean.getColumns().toArray(new String[0]);

        for (int i = 0; i < columns.length; i++) {
            col.put(columns[i], i + 1);
        }
    }

    @Override
    public int getRow() {
        return rowNumber + 1;
    }

    public String getDB() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    private int asColumn(String column) {
        if (col.containsKey(column)) {
            return col.get(column);
        } else {
            throw new RuntimeException("no column " + column + " in columns list " + Arrays.toString(getColumnNames()));
        }
    }

    private void checkValues(String[] columns, Object[] values) throws SQLException {
        if (columns.length != values.length) {
            throw new SQLException("field count mismatched");
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (closed) {
            return false;
        }

        if (rowNumber >= dataBean.getRows().size()) {
            return false;
        }

        if (maxRows != 0 && rowNumber >= maxRows) {
            // no more values
            return false;
        }

        values = dataBean.getRows().get(rowNumber).toArray();
        checkValues(columns, values);
        rowNumber += 1;
        return true;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public CovenantStatement getStatement() {
        return statement;
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public String[] getColumnNames() {
        return columns;
    }

    public String[] getTypes() {
        return types;
    }

    public Map<String, Integer> getCol() {
        return col;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new CovenantResultSetMetaData(this);
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (lastReadColumn == 0) throw new IllegalStateException("You should get something before check nullability");

        // test if current value is null
        return getValue(lastReadColumn) == null;
    }

    private Object[] getValues() {
        return values;
    }

    private Object getValue(int colNum) {
        lastReadColumn = colNum;
        return values[colNum - 1];
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        // with type detection
        Object val = getValue(columnIndex);
        int sqlType = TypeUtils.toSQLTypeWithDetection(types[columnIndex - 1], val);

        if (sqlType == Types.OTHER) {
            return val;
        }

        Class classType = TypeUtils.toClass(sqlType);

        switch (classType.getSimpleName()) {
            case "Boolean":
                return getBoolean(columnIndex);
            case "Integer":
                return getInt(columnIndex);
            case "Long":
                return getLong(columnIndex);
            case "Double":
                return getDouble(columnIndex);
            case "BigDecimal":
                return getBigDecimal(columnIndex);
            case "String":
                return getString(columnIndex);
            case "Float":
                return getFloat(columnIndex);
            case "Date":
                return getDate(columnIndex);
            case "Timestamp":
                return getTimestamp(columnIndex);
            case "Time":
                return getTime(columnIndex);
        }

        return val;
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        // TODO: support auto type cast
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);

        if (val == null) {
            return null;
        }

        return val.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);

        if (val == null) {
            return false;
        }

        if (val instanceof Boolean) {
            return (Boolean) val;
        } else if (val instanceof Number) {
            if (val instanceof BigInteger) {
                return ((BigInteger) val).signum() > 0;
            } else if (val instanceof BigDecimal) {
                return ((BigDecimal) val).signum() > 0;
            } else if (val instanceof Float || val instanceof Double) {
                return ((Number) val).doubleValue() > 0;
            } else {
                return ((Number) val).longValue() > 0;
            }
        } else {
            return BooleanUtils.toBoolean(val.toString());
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);

        if (val == null) {
            return 0;
        }

        if (val instanceof Number) {
            return ((Number) val).byteValue();
        } else {
            return Byte.parseByte(val.toString());
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);

        if (val == null) {
            return 0;
        }

        if (val instanceof Number) {
            return ((Number) val).shortValue();
        } else {
            return Short.parseShort(val.toString());
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);

        if (val == null) {
            return 0;
        }

        if (val instanceof Number) {
            return ((Number) val).intValue();
        } else {
            return Integer.parseInt(val.toString());
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);

        if (val == null) {
            return 0;
        }

        if (val instanceof Number) {
            return ((Number) val).longValue();
        } else {
            return Long.parseLong(val.toString());
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);

        if (val == null) {
            return 0;
        }

        if (val instanceof Number) {
            return ((Number) val).floatValue();
        } else {
            return Float.parseFloat(val.toString());
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);

        if (val == null) {
            return 0;
        }

        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        } else {
            return Double.parseDouble(val.toString());
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);

        if (val == null) {
            return BigDecimal.ZERO;
        }

        if (val instanceof Number) {
            if (val instanceof BigDecimal) {
                return ((BigDecimal) val).add(BigDecimal.ZERO);
            } else if (val instanceof BigInteger) {
                return new BigDecimal((BigInteger) val);
            } else if (val instanceof Float || val instanceof Double) {
                return new BigDecimal(((Number) val).doubleValue());
            } else {
                return new BigDecimal(((Number) val).longValue());
            }
        } else {
            return new BigDecimal(val.toString());
        }
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndex).setScale(scale, BigDecimal.ROUND_HALF_UP);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        // bytes are in string format when pushed from the server
        return getString(columnIndex).getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        try {
            return new Date(DateUtils.parseDate(getString(columnIndex), dateFormats).getTime());
        } catch (ParseException e) {
            throw new SQLException("parse field to date type failed", e);
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        try {
            return new Time(DateUtils.parseDate(getString(columnIndex), dateFormats).getTime());
        } catch (ParseException e) {
            throw new SQLException("parse field to date type failed", e);
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        try {
            return new Timestamp(DateUtils.parseDate(getString(columnIndex), dateFormats).getTime());
        } catch (ParseException e) {
            throw new SQLException("parse field to date type failed", e);
        }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(asColumn(columnLabel), type);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(asColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(asColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(asColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(asColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(asColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(asColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(asColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(asColumn(columnLabel));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(asColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(asColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(asColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(asColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(asColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(asColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(asColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(asColumn(columnLabel), cal);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(asColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(asColumn(columnLabel));
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
