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

package org.apache.hadoop.hive.registry.storage.tool.sql;

public enum DatabaseType {

    MYSQL("mysql"),
    POSTGRES("postgresql"),
    ORACLE("oracle");

    private final String value;

    DatabaseType(String dbType) {
        value = dbType;
    }

    @Override
    public String toString() {
        return value;
    }

    public static DatabaseType fromValue(String otherValue) {
        for (DatabaseType databaseType : values()) {
            if (databaseType.value.equals(otherValue))
                return databaseType;
        }
        throw new IllegalArgumentException("Unknown Database Type : " + otherValue);
    }
}
