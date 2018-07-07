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
package org.apache.hadoop.hive.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Preconditions;

import java.io.Serializable;

/**
 * This class contains schema name and version.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class SchemaVersionKey implements Serializable {

    private static final long serialVersionUID = 1779747592974345866L;
    public static final Integer LATEST_VERSION = -1;

    private String schemaName;
    private Integer version;

    /**
     * Private constructor for Jackson JSON mapping
     */
    @SuppressWarnings("unused")
    private SchemaVersionKey() {
    }

    /**
     * @param schemaName unique schema name
     * @param version    version of the schema
     */
    public SchemaVersionKey(String schemaName, Integer version) {
        Preconditions.checkNotNull(schemaName, "schemaMetadataKey can not be null");
        Preconditions.checkNotNull(version, "version can not be null");
        this.schemaName = schemaName;
        this.version = version;
    }

    /**
     * @return unique schema name
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @return version of the schema
     */
    public Integer getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaVersionKey that = (SchemaVersionKey) o;

        if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null) return false;
        return version != null ? version.equals(that.version) : that.version == null;

    }

    @Override
    public int hashCode() {
        int result = schemaName != null ? schemaName.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaVersionKey{" +
                "schemaName='" + schemaName + '\'' +
                ", version=" + version +
                '}';
    }
}
