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
package org.apache.hadoop.hive.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SerDesPair implements Serializable {
    private static final long serialVersionUID = -1291992612139840722L;

    private String name;
    private String description;
    private String fileId;
    private String serializerClassName;
    private String deserializerClassName;

    public SerDesPair() {
    }

    public SerDesPair(String name,
                      String description,
                      String fileId,
                      String serializerClassName,
                      String deserializerClassName) {
        this.name = name;
        this.description = description;
        this.fileId = fileId;
        this.serializerClassName = serializerClassName;
        this.deserializerClassName = deserializerClassName;
        checkNotNullAndEmpty(name, "name");
        checkNotNullAndEmpty(fileId, "fileId");
        checkNotNullAndEmpty(serializerClassName, "serializerClassName");
        checkNotNullAndEmpty(deserializerClassName, "deserializerClassName");
    }

    private void checkNotNullAndEmpty(String arg, String argName) {
        if(arg == null || arg.trim().isEmpty()) {
            throw new IllegalArgumentException(argName + " can not be null");
        }
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getFileId() {
        return fileId;
    }

    public String getSerializerClassName() {
        return serializerClassName;
    }

    public String getDeserializerClassName() {
        return deserializerClassName;
    }

    @Override
    public String toString() {
        return "SerDesPair{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", fileId='" + fileId + '\'' +
                ", serializerClassName='" + serializerClassName + '\'' +
                ", deserializerClassName='" + deserializerClassName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SerDesPair that = (SerDesPair) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (fileId != null ? !fileId.equals(that.fileId) : that.fileId != null) return false;
        if (serializerClassName != null ? !serializerClassName.equals(that.serializerClassName) : that.serializerClassName != null)
            return false;
        return deserializerClassName != null ? deserializerClassName.equals(that.deserializerClassName) : that.deserializerClassName == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (fileId != null ? fileId.hashCode() : 0);
        result = 31 * result + (serializerClassName != null ? serializerClassName.hashCode() : 0);
        result = 31 * result + (deserializerClassName != null ? deserializerClassName.hashCode() : 0);
        return result;
    }
}
