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
import java.util.Collections;

/**
 * This class is about entity representation to store serializer or deserializer information for a given schema.
 */
public class SerDesInfoStorable {
    public static final String NAME_SPACE = "schema_serdes_info";
    public static final String ID = "id";
    public static final String DESCRIPTION = "description";
    public static final String NAME = "name";
    public static final String SERIALIZER_CLASS_NAME = "serializerClassName";
    public static final String DESERIALIZER_CLASS_NAME = "deserializerClassName";
    public static final String FILE_ID = "fileId";
    public static final String TIMESTAMP = "timestamp";

    public static final Schema SCHEMA = Schema.of(
            Schema.Field.of(ID, Schema.Type.LONG),
            Schema.Field.of(NAME, Schema.Type.STRING),
            Schema.Field.optional(DESCRIPTION, Schema.Type.STRING),
            Schema.Field.of(TIMESTAMP, Schema.Type.LONG),
            Schema.Field.of(SERIALIZER_CLASS_NAME, Schema.Type.STRING),
            Schema.Field.of(DESERIALIZER_CLASS_NAME, Schema.Type.STRING),
            Schema.Field.of(FILE_ID, Schema.Type.STRING)
    );

    /**
     * Unique ID generated for this component.
     */
    protected Long id;

    /**
     * Description about this serializer instance
     */
    private String description;

    /**
     * Name of the serializer
     */
    private String name;

    /**
     * Id of the file stored.
     */
    private String fileId;

    /**
     * Class name of serializer
     */
    private String serializerClassName;

    /**
     * Class name of deserializer
     */
    private String deserializerClassName;

    private Long timestamp;

    public SerDesInfoStorable() {
    }

    public SerDesInfoStorable(SerDesPair serDesPair) {
        name = serDesPair.getName();
        description = serDesPair.getDescription();
        fileId = serDesPair.getFileId();
        serializerClassName = serDesPair.getSerializerClassName();
        deserializerClassName = serDesPair.getDeserializerClassName();
    }



    public void setId(Long id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public String getSerializerClassName() {
        return serializerClassName;
    }

    public void setSerializerClassName(String serializerClassName) {
        this.serializerClassName = serializerClassName;
    }

    public String getDeserializerClassName() {
        return deserializerClassName;
    }

    public void setDeserializerClassName(String serializer) {
        deserializerClassName = serializer;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public SerDesInfo toSerDesInfo() {
        return new SerDesInfo(id,
                              timestamp,
                              new SerDesPair(name, description, fileId, serializerClassName, deserializerClassName));
    }

    @Override
    public String toString() {
        return "SerDesInfoStorable{" +
                "id=" + id +
                ", description='" + description + '\'' +
                ", name='" + name + '\'' +
                ", fileId='" + fileId + '\'' +
                ", serializerClassName='" + serializerClassName + '\'' +
                ", deserializerClassName='" + deserializerClassName + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
