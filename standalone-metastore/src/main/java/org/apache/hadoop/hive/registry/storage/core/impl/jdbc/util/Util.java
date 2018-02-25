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
package org.apache.hadoop.hive.registry.storage.core.impl.jdbc.util;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;

public class Util {
    private static String getSqlTypeName(int sqlType) {
        try {
            Integer val = sqlType;
            for (Field field : Types.class.getFields()) {
                if (val.equals(field.get(null))) {
                    return field.getName();
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Could not get sqlTypeName ", e);
        }
        throw new RuntimeException("Unknown sqlType " + sqlType);
    }

    public static Class getJavaType(int sqlType, int precision) {
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                return String.class;
            case Types.BINARY:
            case Types.VARBINARY:
                return byte[].class;
            case Types.BIT:
                return Boolean.class;
            case Types.TINYINT:
            case Types.SMALLINT:
                return Short.class;
            case Types.INTEGER:
                return Integer.class;
            case Types.BIGINT:
                return Long.class;
            case Types.REAL:
                return Float.class;
            case Types.DOUBLE:
            case Types.FLOAT:
                return Double.class;
            case Types.DATE:
                return Date.class;
            case Types.TIME:
                return Time.class;
            case Types.TIMESTAMP:
                return Timestamp.class;
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return InputStream.class;
            case Types.NUMERIC:
                switch (precision) {
                    case 1:
                        return Boolean.class;
                    case 3:
                        return Byte.class;
                    case 10:
                        return Integer.class;
                    default:
                        return Long.class;
                }
            default:
                throw new RuntimeException("We do not support tables with SqlType: " + getSqlTypeName(sqlType));
        }
    }

    public static void validateJDBCProperties(Map<String, Object> jdbcProps, List<String> propertyNames) {
        if(jdbcProps == null || jdbcProps.isEmpty()) {
            throw new IllegalArgumentException("jdbc properties can neither be null nor empty");
        }

        for (String property : propertyNames) {
            if(!jdbcProps.containsKey(property)) {
                throw new IllegalArgumentException("jdbc properties should contain "+property);
            }
        }
    }
}
