/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.registry.storage.core;


public class OrderByField {
    private final String fieldName;
    private final boolean isDescending;

    private OrderByField(String fieldName, boolean isDescending) {

        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("fieldName argument can neither be empty nor null");
        }
        this.fieldName = fieldName;
        this.isDescending = isDescending;
    }

    public String getFieldName() {
        return fieldName;
    }

    public boolean isDescending() {
        return isDescending;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OrderByField that = (OrderByField) o;

        if (isDescending != that.isDescending) return false;
        return fieldName != null ? fieldName.equals(that.fieldName) : that.fieldName == null;
    }

    @Override
    public int hashCode() {
        int result = fieldName != null ? fieldName.hashCode() : 0;
        result = 31 * result + (isDescending ? 1 : 0);
        return result;
    }

    public static OrderByField of(String fieldName, boolean isDescending) {
        return new OrderByField(fieldName, isDescending);
    }

    public static OrderByField of(String fieldName) {
        return new OrderByField(fieldName, false);
    }

}
