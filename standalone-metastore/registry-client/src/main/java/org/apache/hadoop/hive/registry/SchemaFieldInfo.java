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

import java.io.Serializable;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaFieldInfo implements Serializable {
    private static final long serialVersionUID = -6194661942575334254L;

    public static final String ID = "id";
    public static final String SCHEMA_INSTANCE_ID = "schemaInstanceId";
    public static final String TIMESTAMP = "timestamp";
    public static final String FIELD_NAMESPACE = "fieldNamespace";
    public static final String NAME = "name";
    public static final String TYPE = "type";

    private final Long id;
    private final String namespace;
    private final String name;
    private final String type;

    public SchemaFieldInfo(String namespace, String name, String type) {
        this(null, namespace, name, type);
    }

    public SchemaFieldInfo(Long id, String namespace, String name, String type) {
        this.id = id;
        this.namespace = namespace;
        this.name = name;
        this.type = type;
    }

    public Long getId() {
        return id;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaFieldInfo schemaFieldInfo = (SchemaFieldInfo) o;

        if (id != null ? !id.equals(schemaFieldInfo.id) : schemaFieldInfo.id != null) return false;
        if (namespace != null ? !namespace.equals(schemaFieldInfo.namespace) : schemaFieldInfo.namespace != null) return false;
        if (name != null ? !name.equals(schemaFieldInfo.name) : schemaFieldInfo.name != null) return false;
        return type != null ? type.equals(schemaFieldInfo.type) : schemaFieldInfo.type == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (namespace != null ? namespace.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FieldInfo{" +
                "id=" + id +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
