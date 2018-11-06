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
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

public class CovenantPreparedStatementImpl extends CovenantStatementImpl implements CovenantPreparedStatement {
    private static final Logger LOG = LoggerFactory.getLogger(CovenantPreparedStatement.class);
    private static final SimpleDateFormat dateFormat;
    private static final SimpleDateFormat dateTimeFormat;

    static {
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        TimeZone tzUTC = TimeZone.getTimeZone("UTC");
        dateTimeFormat.setTimeZone(tzUTC);
    }

    private final String sql;
    private final List<String> sqlParts;
    private String[] binds;
    private boolean[] valuesQuote;
    private List<byte[]> batchRows = new ArrayList<>();

    public CovenantPreparedStatementImpl(CloseableHttpClient httpClient, CovenantConnection connection,
                                         CovenantProperties properties, String sql) throws SQLException {
        super(httpClient, connection, properties);
        this.sql = sql;
        this.sqlParts = parseSQL(sql);
        createBinds();
    }

    protected static List<String> parseSQL(String sql) throws SQLException {
        if (sql == null) {
            throw new SQLException("sql statement can't be null");
        }

        List<String> parts = new ArrayList<>();

        boolean afterBackSlash = false, inQuotes = false, inBackQuotes = false;
        boolean inSingleLineComment = false;
        boolean inMultiLineComment = false;
        int partStart = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inSingleLineComment) {
                if (c == '\n') {
                    inSingleLineComment = false;
                }
            } else if (inMultiLineComment) {
                if (c == '*' && sql.length() > i + 1 && sql.charAt(i + 1) == '/') {
                    inMultiLineComment = false;
                    i++;
                }
            } else if (afterBackSlash) {
                afterBackSlash = false;
            } else if (c == '\\') {
                afterBackSlash = true;
            } else if (c == '\'') {
                inQuotes = !inQuotes;
            } else if (c == '`') {
                inBackQuotes = !inBackQuotes;
            } else if (!inQuotes && !inBackQuotes) {
                if (c == '?') {
                    parts.add(sql.substring(partStart, i));
                    partStart = i + 1;
                } else if (c == '-' && sql.length() > i + 1 && sql.charAt(i + 1) == '-') {
                    inSingleLineComment = true;
                    i++;
                } else if (c == '/' && sql.length() > i + 1 && sql.charAt(i + 1) == '*') {
                    inMultiLineComment = true;
                    i++;
                }
            }
        }
        parts.add(sql.substring(partStart));

        return parts;
    }

    private static void checkBinded(String[] binds) throws SQLException {
        int i = 0;
        for (String b : binds) {
            ++i;
            if (b == null) {
                throw new SQLException("Not all parameters binded (placeholder " + i + " is undefined)");
            }
        }
    }

    protected String buildSQL() throws SQLException {
        if (sqlParts.size() == 1) {
            return sqlParts.get(0);
        }
        checkBinded(binds);

        StringBuilder sb = new StringBuilder(sqlParts.get(0));
        for (int i = 1; i < sqlParts.size(); i++) {
            appendBoundValue(sb, i - 1);
            sb.append(sqlParts.get(i));
        }

        return sb.toString();
    }

    private void appendBoundValue(StringBuilder sb, int num) {
        if (valuesQuote[num]) {
            sb.append("'").append(binds[num]).append("'");
        } else if (binds[num].equals("\\N")) {
            sb.append("null");
        } else {
            sb.append(binds[num]);
        }
    }

    private void createBinds() {
        this.binds = new String[this.sqlParts.size() - 1];
        this.valuesQuote = new boolean[this.sqlParts.size() - 1];
    }

    private byte[] buildBinds() throws SQLException {
        checkBinded(binds);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < binds.length; i++) {
            sb.append(binds[i]);
            sb.append(i < binds.length - 1 ? '\t' : '\n');
        }
        return sb.toString().getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate() throws SQLException {
        return 0;
    }

    private void setBind(int parameterIndex, String bind) {
        setBind(parameterIndex, bind, false);
    }

    private void setBind(int paramterIndex, String bind, boolean quote) {
        binds[paramterIndex - 1] = bind;
        valuesQuote[paramterIndex - 1] = quote;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setBind(parameterIndex, "\\N");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setBind(parameterIndex, x ? "1" : "0");
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setBind(parameterIndex, Byte.toString(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setBind(parameterIndex, Short.toString(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setBind(parameterIndex, Integer.toString(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setBind(parameterIndex, Long.toString(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setBind(parameterIndex, Float.toString(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setBind(parameterIndex, Double.toString(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setBind(parameterIndex, x.toPlainString());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setBind(parameterIndex, CovenantUtil.escape(x), x != null);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setBind(parameterIndex, new String(x, Charset.forName("UTF-8")));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setBind(parameterIndex, dateFormat.format(x), true);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setBind(parameterIndex, dateTimeFormat.format(x), true);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setBind(parameterIndex, dateTimeFormat.format(x), true);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearParameters() throws SQLException {
        Arrays.fill(binds, null);
        Arrays.fill(valuesQuote, false);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.OTHER);
        } else {
            if (x instanceof Byte) {
                setInt(parameterIndex, ((Byte) x).intValue());
            } else if (x instanceof String) {
                setString(parameterIndex, (String) x);
            } else if (x instanceof BigDecimal) {
                setBigDecimal(parameterIndex, (BigDecimal) x);
            } else if (x instanceof Short) {
                setShort(parameterIndex, (Short) x);
            } else if (x instanceof Integer) {
                setInt(parameterIndex, (Integer) x);
            } else if (x instanceof Long) {
                setLong(parameterIndex, (Long) x);
            } else if (x instanceof Float) {
                setFloat(parameterIndex, (Float) x);
            } else if (x instanceof Double) {
                setDouble(parameterIndex, (Double) x);
            } else if (x instanceof byte[]) {
                setBytes(parameterIndex, (byte[]) x);
            } else if (x instanceof Date) {
                setDate(parameterIndex, (Date) x);
            } else if (x instanceof Time) {
                setTime(parameterIndex, (Time) x);
            } else if (x instanceof Timestamp) {
                setTimestamp(parameterIndex, (Timestamp) x);
            } else if (x instanceof java.util.Date) {
                setTimestamp(parameterIndex, new Timestamp(((java.util.Date) x).getTime()));
            } else if (x instanceof Boolean) {
                setBoolean(parameterIndex, (Boolean) x);
            } else if (x instanceof InputStream) {
                setBinaryStream(parameterIndex, (InputStream) x, -1);
            } else if (x instanceof Blob) {
                setBlob(parameterIndex, (Blob) x);
            } else if (x instanceof Clob) {
                setClob(parameterIndex, (Clob) x);
            } else if (x instanceof BigInteger) {
                setBind(parameterIndex, x.toString());
            } else {
                throw new SQLDataException("Can't bind object of class " + x.getClass().getCanonicalName());
            }
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(buildSQL());
    }

    @Override
    public void addBatch() throws SQLException {
        // TODO: implement batch processing feature
        batchRows.add(buildBinds());
        createBinds();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
