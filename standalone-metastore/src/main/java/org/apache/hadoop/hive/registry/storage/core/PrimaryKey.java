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
package org.apache.hadoop.hive.registry.storage.core;

import org.apache.hadoop.hive.registry.common.Schema;

import java.util.Map;

/**
 * An instance of this class represents what fields defines the primary key columns of a {@code Storable} entity.
 */
public class PrimaryKey {

    /**
     * The fieldsToVal map has {@code Schema.Field} as the key which defines the name of columns and their types that forms
     * the primary key. The value, if not null represents the actual value of that column in a stored instance. for example
     * if you have a storable entity called "Employee" with primaryKey as "employeeId" an instance of this class
     * <pre>
     *     PrimaryKey {
     *         fieldsToVal = {
     *              new Field("employeeId", Field.Type.String) -> 1;
     *         }
     *     }
     * </pre>
     *
     * represents that employeeId is the primary key with type String and the value is actually referring to the row with
     * employeeId = 1.
     */
    private final Map<Schema.Field, Object> fieldsToVal;

    public PrimaryKey(Map<Schema.Field, Object> fieldsToVal) {
        this.fieldsToVal = fieldsToVal;
    }

    @Override
    public String toString() {
        return "StorableId{" +
                "fieldsToVal=" + fieldsToVal +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrimaryKey)) return false;

        PrimaryKey that = (PrimaryKey) o;

        return fieldsToVal.equals(that.fieldsToVal);

    }

    @Override
    public int hashCode() {
        return fieldsToVal.hashCode();
    }

    public Map<Schema.Field, Object> getFieldsToVal() {
        return fieldsToVal;
    }
}
