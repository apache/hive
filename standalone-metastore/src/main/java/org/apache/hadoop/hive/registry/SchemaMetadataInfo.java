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
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.SchemaCompatibility;
import org.apache.hadoop.hive.metastore.api.SchemaValidation;

import java.io.Serializable;

/**
 * This class encapsulates metadata about a schema which includes group, name, type, description and compatibility.
 * There can be only one instance with the same name.
 * New versions of the schema can be registered for the given {@link SchemaMetadataInfo} by giving {@link SchemaVersion} instances.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class SchemaMetadataInfo implements Serializable {

    private static final long serialVersionUID = 8083699103887496439L;

    /**
     * metadata about the schema
     */
    private SchemaMetadata schemaMetadata;

    private Long id;

    private Long timestamp;

    @SuppressWarnings("unused")
    private SchemaMetadataInfo() { /* Private constructor for Jackson JSON mapping */}

    /**
     * @param iSchema Schema metadata
     */

    public SchemaMetadataInfo(ISchema iSchema) {
       this.schemaMetadata = new SchemaMetadata.Builder(iSchema.getName())
                .type(iSchema.getSchemaType().toString())
                .schemaGroup(iSchema.getSchemaGroup())
                .compatibility(org.apache.hadoop.hive.registry.SchemaCompatibility.values()[iSchema.getCompatibility().ordinal()])
                .validationLevel(SchemaValidationLevel.values()[iSchema.getValidationLevel().ordinal()])
                .description(iSchema.getDescription())
                .evolve(iSchema.isCanEvolve())
                .build();
       this.id = iSchema.getSchemaId();
       this.timestamp = iSchema.getTimestamp();
    }

    public SchemaMetadataInfo(SchemaMetadata schemaMetadata) {
        this(schemaMetadata, null, System.currentTimeMillis());
    }

    public SchemaMetadataInfo(SchemaMetadata schemaMetadata,
                       Long id,
                       Long timestamp) {
        Preconditions.checkNotNull(schemaMetadata, "schemaMetadata can not be null");
        this.schemaMetadata = schemaMetadata;
        this.id = id;
        this.timestamp = timestamp;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) { this.id = id; }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

    public SchemaMetadata getSchemaMetadata() {
        return schemaMetadata;
    }

    public void setSchemaMetadata(SchemaMetadata schemaMetadata) { this.schemaMetadata = schemaMetadata; }

    public ISchema buildThriftSchemaRequest() {
        ISchema thriftSchema = new ISchema();
        thriftSchema.setName(schemaMetadata.getName());
        thriftSchema.setDescription(schemaMetadata.getDescription());
        thriftSchema.setCanEvolve(schemaMetadata.isEvolve());
        thriftSchema.setCompatibility(SchemaCompatibility.findByValue(schemaMetadata.getCompatibility().ordinal()));
        thriftSchema.setSchemaGroup(schemaMetadata.getSchemaGroup());
        thriftSchema.setValidationLevel(SchemaValidation.findByValue(schemaMetadata.getValidationLevel().ordinal()));
        return thriftSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaMetadataInfo that = (SchemaMetadataInfo) o;

        if (schemaMetadata != null ? !schemaMetadata.equals(that.schemaMetadata) : that.schemaMetadata != null)
            return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        return timestamp != null ? timestamp.equals(that.timestamp) : that.timestamp == null;

    }

    @Override
    public int hashCode() {
        int result = schemaMetadata != null ? schemaMetadata.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaMetadataInfo{" +
                "schemaMetadata=" + schemaMetadata +
                ", id=" + id +
                ", timestamp=" + timestamp +
                '}';
    }
}
