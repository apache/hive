/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.statement;

import org.apache.hadoop.hive.registry.common.Schema;
import org.apache.hadoop.hive.registry.storage.core.exception.StorageException;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class DefaultStorageDataTypeContext implements StorageDataTypeContext {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStorageDataTypeContext.class);

    @Override
    public void setPreparedStatementParams(PreparedStatement preparedStatement,
                                           Schema.Type type, int index, Object val) throws SQLException {
        if (val == null) {
            preparedStatement.setNull(index, getSqlType(type));
            return;
        }

        switch (type) {
            case BOOLEAN:
                preparedStatement.setBoolean(index, (Boolean) val);
                break;
            case BYTE:
                preparedStatement.setByte(index, (Byte) val);
                break;
            case SHORT:
                preparedStatement.setShort(index, (Short) val);
                break;
            case INTEGER:
                preparedStatement.setInt(index, (Integer) val);
                break;
            case LONG:
                preparedStatement.setLong(index, (Long) val);
                break;
            case FLOAT:
                preparedStatement.setFloat(index, (Float) val);
                break;
            case DOUBLE:
                preparedStatement.setDouble(index, (Double) val);
                break;
            case STRING:
                preparedStatement.setString(index, (String) val);
                break;
            case BINARY:
                preparedStatement.setBytes(index, (byte[]) val);
                break;
            case BLOB:
                preparedStatement.setBinaryStream(index, (InputStream) val);
                break;
            case NESTED:
            case ARRAY:
                preparedStatement.setObject(index, val);    //TODO check this
                break;
        }
    }

    @Override
    public Map<String, Object> getMapWithRowContents(ResultSet resultSet, ResultSetMetaData rsMetadata) throws SQLException {
        final Map<String, Object> map = new HashMap<>();
        final int columnCount = rsMetadata.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            final String columnLabel = rsMetadata.getColumnLabel(i);
            final int columnType = rsMetadata.getColumnType(i);
            final int columnPrecision = rsMetadata.getPrecision(i);
            final Class columnJavaType = Util.getJavaType(columnType, columnPrecision);
            map.put(columnLabel, getJavaObject(columnJavaType, columnLabel, resultSet));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Row for ResultSet [{}] with metadata [{}] generated Map [{}]", resultSet, rsMetadata, map);
        }
        return map;
    }

    protected int getSqlType(Schema.Type type) {
        switch (type) {
            case BOOLEAN:
                return Types.BOOLEAN;
            case BYTE:
                return Types.TINYINT;
            case SHORT:
                return Types.SMALLINT;
            case INTEGER:
                return Types.INTEGER;
            case LONG:
                return Types.BIGINT;
            case FLOAT:
                return Types.REAL;
            case DOUBLE:
                return Types.DOUBLE;
            case STRING:
                // it might be a VARCHAR or LONGVARCHAR
                return Types.VARCHAR;
            case BINARY:
                // it might be a VARBINARY or LONGVARBINARY
                return Types.VARBINARY;
            case BLOB:
                return Types.BLOB;
            case NESTED:
            case ARRAY:
                return Types.JAVA_OBJECT;
            default:
                throw new IllegalArgumentException("Not supported type: " + type);
        }
    }

    protected Object getJavaObject(Class columnJavaType, String columnLabel, ResultSet resultSet) throws SQLException {
        if (columnJavaType.equals(String.class)) {
            return resultSet.getString(columnLabel);
        } else if (columnJavaType.equals(Byte.class)) {
            return resultSet.getByte(columnLabel);
        } else if (columnJavaType.equals(Integer.class)) {
            return resultSet.getInt(columnLabel);
        } else if (columnJavaType.equals(Double.class)) {
            return resultSet.getDouble(columnLabel);
        } else if (columnJavaType.equals(Float.class)) {
            return resultSet.getFloat(columnLabel);
        } else if (columnJavaType.equals(Short.class)) {
            return resultSet.getShort(columnLabel);
        } else if (columnJavaType.equals(Boolean.class)) {
            return resultSet.getBoolean(columnLabel);
        } else if (columnJavaType.equals(byte[].class)) {
            return resultSet.getBytes(columnLabel);
        } else if (columnJavaType.equals(Long.class)) {
            return resultSet.getLong(columnLabel);
        } else if (columnJavaType.equals(Date.class)) {
            return resultSet.getDate(columnLabel);
        } else if (columnJavaType.equals(Time.class)) {
            return resultSet.getTime(columnLabel);
        } else if (columnJavaType.equals(Timestamp.class)) {
            return resultSet.getTimestamp(columnLabel);
        } else if (columnJavaType.equals(InputStream.class)) {
            Blob blob = resultSet.getBlob(columnLabel);
            return blob != null ? blob.getBinaryStream() : null;
        } else {
            throw new StorageException("type =  [" + columnJavaType + "] for column [" + columnLabel + "] not supported.");
        }
    }

}
