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
package org.apache.hadoop.hive.registry.serdes.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.registry.SchemaResolver;
import org.apache.hadoop.hive.registry.SchemaVersionKey;
import org.apache.hadoop.hive.registry.SchemaVersionRetriever;
import org.apache.hadoop.hive.registry.common.errors.CyclicSchemaDependencyException;
import org.apache.hadoop.hive.registry.common.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.common.errors.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.avro.Schema.Type.RECORD;

/**
 * Avro implementation of {@link SchemaResolver} which resolves all the dependent schemas and builds an effective schema.
 *
 * List of dependent schemas can be added with `includeSchemas` attribute in avro schema. This contains name and version
 * of each schema as mentioned below.
 * - name       : unique name of that schema in schema registry which is {@link org.apache.hadoop.hive.registry.SchemaMetadata#name}.
 * - version    : version number of the schema being used which is {@link SchemaVersionKey#version}.
 *                When this property is not mentioned then it is considered to be latest version of that schema when
 *                it builds effective schema.
 *
 * Example of Avro schema containing dependencies is given below.
 *
 * account schema:
 * <pre> {@code
 *
    {
        "name": "account",
        "namespace": "org.apache.hadoop.hive.registry.types",
        "includeSchemas": [
            {
                "name": "utils",
                "version": "2",
            }
        ],
        "type": "record",
        "fields": [
            {
                "name": "name",
                "type": "string"
            },
            {
                "name": "id",
                "type": "org.apache.hadoop.hive.datatypes.uuid"
            }
        ]
    }
 * }</pre>
 *
 * dependent utils schema:
 * <pre> {@code
 * {
     "name": "uuid",
     "type": "record",
     "namespace": "org.apache.hadoop.hive.datatypes",
     "doc": "A Universally Unique Identifier, in canonical form in lowercase. This is generated from java.util.UUID Example: de305d54-75b4-431b-adb2-eb6b9e546014",
     "fields": [
         {
             "name": "value",
             "type": "string",
             "default": ""
         }
     ]
   }
 *
 * }</pre>
 *
 */
public class AvroSchemaResolver implements SchemaResolver {

    private enum SchemaParsingState {
        PARSING, PARSED
    }

    private final SchemaVersionRetriever schemaVersionRetriever;

    public AvroSchemaResolver(SchemaVersionRetriever schemaVersionRetriever) {
        this.schemaVersionRetriever = schemaVersionRetriever;
    }

    @Override
    public String resolveSchema(SchemaVersionKey schemaVersionKey) throws InvalidSchemaException, SchemaNotFoundException {
        Map<String, SchemaParsingState> schemaParsingStates = new HashMap<>();
        schemaParsingStates.put(schemaVersionKey.getSchemaName(), SchemaParsingState.PARSING);
        return getResultantSchema(schemaVersionKey, schemaParsingStates);
    }

    public String resolveSchema(String schemaText) throws InvalidSchemaException, SchemaNotFoundException {
        Map<String, SchemaParsingState> schemaParsingStates = new HashMap<>();
        return getResultantSchema(schemaText, schemaParsingStates);
    }

    private String getResultantSchema(SchemaVersionKey schemaVersionKey,
                                      Map<String, SchemaParsingState> schemaParsingStates)
            throws InvalidSchemaException, SchemaNotFoundException {
        String schemaText = schemaVersionRetriever.retrieveSchemaVersion(schemaVersionKey).getSchemaText();
        return getResultantSchema(schemaText, schemaParsingStates);
    }

    private String getResultantSchema(String schemaText, Map<String, SchemaParsingState> schemaParsingStates)
            throws InvalidSchemaException, SchemaNotFoundException {
        Map<String, Schema> complexTypes = traverseIncludedSchemaTypes(schemaText, schemaParsingStates);

        Schema.Parser parser = new Schema.Parser();
        parser.addTypes(complexTypes);
        Schema schema = parser.parse(schemaText);
        Set<String> visitingTypes = new HashSet<>();
        Schema updatedSchema = handleUnionFieldsWithNull(schema, visitingTypes);

        return (schema == updatedSchema && complexTypes.isEmpty()) ? schemaText : updatedSchema.toString();
    }

    public Schema handleUnionFieldsWithNull(Schema schema, Set<String> visitingTypes) {
        if (visitingTypes.contains(schema.getFullName())) {
            return schema;
        }
        visitingTypes.add(schema.getFullName());

        Schema updatedRootSchema = schema;
        if (schema.getType() == RECORD) {
            List<Schema.Field> fields = updatedRootSchema.getFields();
            List<Schema.Field> updatedFields = new ArrayList<>(fields.size());
            boolean hasUnionType = false;

            for (Schema.Field field : fields) {
                Schema fieldSchema = field.schema();
                // check for union

                boolean currentFieldTypeIsUnion = fieldSchema.getType() == Schema.Type.UNION;
                if (currentFieldTypeIsUnion) {
                    // check for the fields with in union
                    // if it is union and first type is null then set default value as null
                    if (fieldSchema.getTypes().get(0).getType() == Schema.Type.NULL) {
                        hasUnionType = true;
                    }
                } else {
                    // go through non-union fields, which may be records
                    Schema updatedFieldSchema = handleUnionFieldsWithNull(fieldSchema, visitingTypes);
                    if (fieldSchema != updatedFieldSchema) {
                        hasUnionType = true;
                    }
                }
                updatedFields.add(new Schema.Field(field.name(),
                                                   fieldSchema,
                                                   field.doc(),
                                                   currentFieldTypeIsUnion ? NullNode.getInstance() : field.defaultValue(),
                                                   field.order()));
            }

            if (hasUnionType) {
                updatedRootSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
                updatedRootSchema.setFields(updatedFields);
                for (String alias : schema.getAliases()) {
                    updatedRootSchema.addAlias(alias);
                }
                for (Map.Entry<String, org.codehaus.jackson.JsonNode> nodeEntry : schema.getJsonProps().entrySet()) {
                    updatedRootSchema.addProp(nodeEntry.getKey(), nodeEntry.getValue());
                }
            }
        }

        return updatedRootSchema;
    }

    private Schema updateUnionFields(Schema schema) {
        Schema updatedSchema = schema;
        List<Schema.Field> fields = schema.getFields();
        boolean hasUnionType = false;
        List<Schema.Field> updatedFields = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            Schema fieldSchema = field.schema();
            Schema.Field updatedField = field;
            // if it is union and first type is null then set default value as null
            if (fieldSchema.getType() == Schema.Type.UNION &&
                    fieldSchema.getTypes().get(0).getType() == Schema.Type.NULL) {
                updatedField = new Schema.Field(field.name(), fieldSchema, field.doc(), NullNode.getInstance(), field.order());
                hasUnionType = true;
            }
            updatedFields.add(updatedField);
        }

        if (hasUnionType) {
            updatedSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
            updatedSchema.setFields(updatedFields);
            for (String alias : schema.getAliases()) {
                updatedSchema.addAlias(alias);
            }
            for (Map.Entry<String, org.codehaus.jackson.JsonNode> nodeEntry : schema.getJsonProps().entrySet()) {
                updatedSchema.addProp(nodeEntry.getKey(), nodeEntry.getValue());
            }
        }

        return updatedSchema;
    }

    private Map<String, Schema> traverseIncludedSchemaTypes(String schemaText,
                                                            Map<String, SchemaParsingState> schemaParsingStates)
            throws InvalidSchemaException, SchemaNotFoundException {
        List<SchemaVersionKey> includedSchemaVersions = getIncludedSchemaVersions(schemaText);

        if (includedSchemaVersions == null || includedSchemaVersions.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Schema> schemaTypes = new HashMap<>();
        for (SchemaVersionKey schemaVersionKey : includedSchemaVersions) {
            Map<String, Schema> collectedSchemas = collectSchemaTypes(schemaVersionKey, schemaParsingStates);
            if (collectedSchemas != null) {
                schemaTypes.putAll(collectedSchemas);
            }
        }

        return schemaTypes;
    }

    private Map<String, Schema> collectSchemaTypes(SchemaVersionKey schemaVersionKey,
                                                   Map<String, SchemaParsingState> schemaParsingStates)
            throws SchemaNotFoundException, InvalidSchemaException {

        String schemaName = schemaVersionKey.getSchemaName();
        SchemaParsingState schemaParsingState = schemaParsingStates.putIfAbsent(schemaName, SchemaParsingState.PARSING);

        // if it is already parsed then the respective schema types would have been already collected.
        if (SchemaParsingState.PARSED == schemaParsingState) {
            return null;
        }

        // if it is in parsing state earlier and it is visted again then ther eis circular dependency!!
        if (SchemaParsingState.PARSING == schemaParsingState) {
            throw new CyclicSchemaDependencyException("Cyclic dependency of schema imports with schema [" + schemaName + "]");
        }

        // this schema is not yet parsed till now
        if (schemaParsingState == null) {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(getResultantSchema(schemaVersionKey, schemaParsingStates));
            Map<String, Schema> complexTypes = new HashMap<>();
            collectComplexTypes(schema, complexTypes);
            schemaParsingStates.put(schemaName, SchemaParsingState.PARSED);
            return complexTypes;
        }

        throw new IllegalStateException("Schema parsing with schema version " + schemaVersionKey + " is in invalid state!!");
    }

    private void collectComplexTypes(Schema schema, Map<String, Schema> complexTypes) {
        switch (schema.getType()) {
            case RECORD:
                complexTypes.put(schema.getFullName(), schema);
                List<Schema.Field> fields = schema.getFields();
                for (Schema.Field field : fields) {
                    Schema fieldSchema = field.schema();
                    collectComplexTypes(fieldSchema, complexTypes);
                }
                break;
            case ARRAY:
                complexTypes.put(schema.getFullName(), schema);
                collectComplexTypes(schema.getElementType(), complexTypes);
                break;
            case UNION:
                complexTypes.put(schema.getFullName(), schema);
                List<Schema> unionSchemas = schema.getTypes();
                for (Schema schemaEntry : unionSchemas) {
                    collectComplexTypes(schemaEntry, complexTypes);
                }
                break;
            case MAP:
                complexTypes.put(schema.getFullName(), schema);
                collectComplexTypes(schema.getValueType(), complexTypes);
                break;
            default:
        }
    }

    private List<SchemaVersionKey> getIncludedSchemaVersions(String schemaText) throws InvalidSchemaException {
        JsonNode jsonNode = null;
        try {
            jsonNode = new ObjectMapper().readTree(schemaText);
        } catch (IOException e) {
            throw new InvalidSchemaException(e);
        }
        JsonNode includeSchemaNodes = jsonNode.get("includeSchemas");
        List<SchemaVersionKey> includedSchemaVersions = new ArrayList<>();
        if (includeSchemaNodes != null) {
            if (!includeSchemaNodes.isArray()) {
                throw new InvalidSchemaException("includeSchemas should be an array of strings");
            }

            for (JsonNode includeSchema : includeSchemaNodes) {
                String name = includeSchema.get("name").asText();
                JsonNode versionNode = includeSchema.get("version");
                int version = versionNode != null ? versionNode.asInt() : SchemaVersionKey.LATEST_VERSION;
                includedSchemaVersions.add(new SchemaVersionKey(name, version));
            }
        }
        return includedSchemaVersions;
    }
}
