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


public class SchemaProviderInfo {
    private String type;
    private String name;
    private String description;
    private String defaultSerializerClassName;
    private String defaultDeserializerClassName;

    private SchemaProviderInfo() {
    }

    public SchemaProviderInfo(String type, String name, String description, String defaultSerializerClassName,
                              String defaultDeserializerClassName) {
        this.type = type;
        this.name = name;
        this.description = description;
        this.defaultSerializerClassName = defaultSerializerClassName;
        this.defaultDeserializerClassName = defaultDeserializerClassName;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultSerializerClassName() {
        return defaultSerializerClassName;
    }

    public String getDefaultDeserializerClassName() {
        return defaultDeserializerClassName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaProviderInfo that = (SchemaProviderInfo) o;

        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (defaultSerializerClassName != null ? !defaultSerializerClassName.equals(that.defaultSerializerClassName) : that.defaultSerializerClassName != null)
            return false;
        return defaultDeserializerClassName != null ? defaultDeserializerClassName.equals(that.defaultDeserializerClassName) : that.defaultDeserializerClassName == null;

    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (defaultSerializerClassName != null ? defaultSerializerClassName.hashCode() : 0);
        result = 31 * result + (defaultDeserializerClassName != null ? defaultDeserializerClassName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaProviderInfo{" +
                "type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", defaultSerializerClassName='" + defaultSerializerClassName + '\'' +
                ", defaultDeserializerClassName='" + defaultDeserializerClassName + '\'' +
                '}';
    }
}
