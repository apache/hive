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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.hive.registry.common.Schema;
import org.apache.hadoop.hive.registry.storage.core.PrimaryKey;
import org.apache.hadoop.hive.registry.storage.core.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchemaFieldInfoStorable extends AbstractStorable {
    public static final String STORABLE_NAME_SPACE = "schema_field_info";

    private Long id;
    private String fieldNamespace;
    private String name;
    private String type;
    private Long schemaInstanceId;
    private Long timestamp;

    public SchemaFieldInfoStorable() {
    }

    public SchemaFieldInfoStorable(Long id) {
        this.id = id;
    }

    @Override
    public String getNameSpace() {
        return STORABLE_NAME_SPACE;
    }

    @Override
    @JsonIgnore
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> values = new HashMap<>();
        values.put(new Schema.Field(SchemaFieldInfo.ID, Schema.Type.LONG), id);
        return new PrimaryKey(values);
    }

    @Override
    public Schema getSchema() {
        return Schema.of(
                Schema.Field.of(SchemaFieldInfo.ID, Schema.Type.LONG),
                Schema.Field.of(SchemaFieldInfo.SCHEMA_INSTANCE_ID, Schema.Type.LONG),
                Schema.Field.optional(SchemaFieldInfo.FIELD_NAMESPACE, Schema.Type.STRING),
                Schema.Field.of(SchemaFieldInfo.NAME, Schema.Type.STRING),
                Schema.Field.of(SchemaFieldInfo.TYPE, Schema.Type.STRING),
                Schema.Field.of(SchemaFieldInfo.TIMESTAMP, Schema.Type.LONG)
        );

    }

    @Override
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFieldNamespace() {
        return fieldNamespace;
    }

    public void setFieldNamespace(String fieldNamespace) {
        this.fieldNamespace = fieldNamespace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getSchemaInstanceId() {
        return schemaInstanceId;
    }

    public void setSchemaInstanceId(Long schemaInstanceId) {
        this.schemaInstanceId = schemaInstanceId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public static SchemaFieldInfoStorable fromSchemaFieldInfo(SchemaFieldInfo schemaFieldInfo, Long id) {
        SchemaFieldInfoStorable schemaFieldInfoStorable = new SchemaFieldInfoStorable(id);
        schemaFieldInfoStorable.setFieldNamespace(schemaFieldInfo.getNamespace());
        schemaFieldInfoStorable.setName(schemaFieldInfo.getName());
        schemaFieldInfoStorable.setType(schemaFieldInfo.getType());

        return schemaFieldInfoStorable;

    }

    @Override
    public String toString() {
        return "SchemaFieldInfoStorable{" +
                "id=" + id +
                ", fieldNamespace='" + fieldNamespace + '\'' +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", schemaInstanceId=" + schemaInstanceId +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaFieldInfoStorable that = (SchemaFieldInfoStorable) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (fieldNamespace != null ? !fieldNamespace.equals(that.fieldNamespace) : that.fieldNamespace != null)
            return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (schemaInstanceId != null ? !schemaInstanceId.equals(that.schemaInstanceId) : that.schemaInstanceId != null)
            return false;
        return timestamp != null ? timestamp.equals(that.timestamp) : that.timestamp == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (fieldNamespace != null ? fieldNamespace.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (schemaInstanceId != null ? schemaInstanceId.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
