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
import org.apache.hadoop.hive.registry.storage.core.Storable;
import org.apache.hadoop.hive.registry.storage.core.catalog.AbstractStorable;

import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class SchemaMetadataStorable extends AbstractStorable {
    public static final String NAME_SPACE = "schema_metadata_info";
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String SCHEMA_GROUP = "schemaGroup";
    public static final String COMPATIBILITY = "compatibility";
    public static final String VALIDATION_LEVEL = "validationLevel";
    public static final String TYPE = "type";
    public static final String TIMESTAMP = "timestamp";
    public static final String EVOLVE = "evolve";

    public static final Schema.Field DESCRIPTION_FIELD = Schema.Field.of(DESCRIPTION, Schema.Type.STRING);
    public static final Schema.Field NAME_FIELD = Schema.Field.of(NAME, Schema.Type.STRING);
    public static final Schema.Field SCHEMA_GROUP_FIELD = Schema.Field.of(SCHEMA_GROUP, Schema.Type.STRING);
    public static final Schema.Field TYPE_FIELD = Schema.Field.of(TYPE, Schema.Type.STRING);

    /**
     * Unique ID generated for this component.
     */
    private Long id;

    /**
     * Schema type which can be avro, protobuf or json etc. User can have a unique constraint with (type, name) values.
     */
    private String type;


    /**
     * Schema group for which this schema metadata belongs to.
     */
    private String schemaGroup;

    /**
     * Given name of the schema.
     * Unique constraint with (name, group, type)
     */
    private String name;

    /**
     * Description of the schema.
     */
    private String description;

    /**
     * Time at which this schema was created/updated.
     */
    private Long timestamp;

    /**
     * Compatibility of this schema instance
     */
    private SchemaCompatibility compatibility = SchemaCompatibility.DEFAULT_COMPATIBILITY;


    /**
     * Validation level of this schema instance
     */
    private SchemaValidationLevel validationLevel = SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL;

    /**
     * Whether this can have evolving schemas or not. If false, this can have only one version of the schema.
     */
    private Boolean evolve;

    public SchemaMetadataStorable() {
    }

    @Override
    @JsonIgnore
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @Override
    @JsonIgnore
    public PrimaryKey getPrimaryKey() {
        return new PrimaryKey(Collections.<Schema.Field, Object>singletonMap(NAME_FIELD, name));
    }

    @Override
    @JsonIgnore
    public Schema getSchema() {
        return Schema.of(
                Schema.Field.of(ID, Schema.Type.LONG),
                NAME_FIELD,
                DESCRIPTION_FIELD,
                SCHEMA_GROUP_FIELD,
                TYPE_FIELD,
                Schema.Field.of(COMPATIBILITY, Schema.Type.STRING),
                Schema.Field.of(VALIDATION_LEVEL, Schema.Type.STRING),
                Schema.Field.of(TIMESTAMP, Schema.Type.LONG),
                Schema.Field.of(EVOLVE, Schema.Type.BOOLEAN)
        );
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> values = super.toMap();
        values.put(COMPATIBILITY, compatibility.name());
        values.put(VALIDATION_LEVEL, validationLevel.name());
        return values;
    }

    @Override
    public Storable fromMap(Map<String, Object> map) {
        String compatibilityName = (String) map.remove(COMPATIBILITY);
        compatibility = SchemaCompatibility.valueOf(compatibilityName);

        String validationLevelName = (String) map.remove(VALIDATION_LEVEL);
        validationLevel = SchemaValidationLevel.valueOf(validationLevelName);

        super.fromMap(map);

        return this;
    }

    @Override
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSchemaGroup() {
        return schemaGroup;
    }

    public void setSchemaGroup(String schemaGroup) {
        this.schemaGroup = schemaGroup;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public SchemaCompatibility getCompatibility() {
        return compatibility;
    }

    public void setCompatibility(SchemaCompatibility compatibility) {
        this.compatibility = compatibility;
    }

    public SchemaValidationLevel getValidationLevel() {
        return validationLevel;
    }

    public void setValidationLevel(SchemaValidationLevel validationLevel) {
        this.validationLevel = validationLevel;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getEvolve() {
        return evolve;
    }

    public void setEvolve(Boolean evolve) {
        this.evolve = evolve;
    }

    public static SchemaMetadataStorable fromSchemaMetadataInfo(SchemaMetadataInfo schemaMetadataInfo) {
        SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
        SchemaMetadataStorable schemaMetadataStorable = updateSchemaMetadata(new SchemaMetadataStorable(), schemaMetadata);
        schemaMetadataStorable.setId(schemaMetadataInfo.getId());
        schemaMetadataStorable.setTimestamp(schemaMetadataInfo.getTimestamp());
        return schemaMetadataStorable;
    }

    public static SchemaMetadataStorable updateSchemaMetadata(SchemaMetadataStorable schemaMetadataStorable, SchemaMetadata schemaMetadata) {
        schemaMetadataStorable.setType(schemaMetadata.getType());
        schemaMetadataStorable.setSchemaGroup(schemaMetadata.getSchemaGroup());
        schemaMetadataStorable.setName(schemaMetadata.getName());
        schemaMetadataStorable.setDescription(schemaMetadata.getDescription());
        schemaMetadataStorable.setCompatibility(schemaMetadata.getCompatibility());
        schemaMetadataStorable.setValidationLevel(schemaMetadata.getValidationLevel());
        schemaMetadataStorable.setEvolve(schemaMetadata.isEvolve());
        return schemaMetadataStorable;
    }

    public SchemaMetadataInfo toSchemaMetadataInfo() {
        SchemaMetadata schemaMetadata = toSchemaMetadata();

        return new SchemaMetadataInfo(schemaMetadata, getId(), getTimestamp());
    }

    public SchemaMetadata toSchemaMetadata() {
        return new SchemaMetadata.Builder(getName())
                .type(getType())
                .schemaGroup(getSchemaGroup())
                .compatibility(getCompatibility())
                .validationLevel(getValidationLevel())
                .description(getDescription())
                .evolve(getEvolve())
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaMetadataStorable that = (SchemaMetadataStorable) o;

        if (evolve != that.evolve) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (schemaGroup != null ? !schemaGroup.equals(that.schemaGroup) : that.schemaGroup != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        if (validationLevel != null ? !validationLevel.equals(that.validationLevel) : that.validationLevel != null) return false;
        return compatibility == that.compatibility;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (schemaGroup != null ? schemaGroup.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (compatibility != null ? compatibility.hashCode() : 0);
        result = 31 * result + (validationLevel != null ? validationLevel.hashCode() : 0);
        result = 31 * result + (evolve ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaMetadataStorable{" +
                "id=" + id +
                ", type='" + type + '\'' +
                ", schemaGroup='" + schemaGroup + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", timestamp=" + timestamp +
                ", compatibility=" + compatibility +
                ", validationLevel=" + validationLevel +
                ", evolve=" + evolve +
                '}';
    }
}
