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


public class StorableKey {
    private final PrimaryKey primaryKey;
    private final String nameSpace;

    public StorableKey(String nameSpace, PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        this.nameSpace = nameSpace;
    }

    /**
     * @return primary key
     */
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    /**
     * @return the namespace associated with this key
     */
    public String getNameSpace() {
        return nameSpace;
    }

    // TODO: apply some syntax formatting guidelines
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StorableKey that = (StorableKey) o;

        if (primaryKey != null ? !primaryKey.equals(that.primaryKey) : that.primaryKey != null) return false;
        return !(nameSpace != null ? !nameSpace.equals(that.nameSpace) : that.nameSpace != null);

    }

    @Override
    public int hashCode() {
        int result = primaryKey != null ? primaryKey.hashCode() : 0;
        result = 31 * result + (nameSpace != null ? nameSpace.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "StorableKey{" +
                "primaryKey=" + primaryKey +
                ", nameSpace='" + nameSpace + '\'' +
                '}';
    }
}
