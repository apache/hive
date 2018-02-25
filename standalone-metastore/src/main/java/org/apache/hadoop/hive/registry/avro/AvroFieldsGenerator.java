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
package org.apache.hadoop.hive.registry.avro;

import org.apache.hadoop.hive.registry.SchemaFieldInfo;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class AvroFieldsGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(AvroFieldsGenerator.class);

    public AvroFieldsGenerator() {
    }

    public List<SchemaFieldInfo> generateFields(Schema rootSchema) {
        List<SchemaFieldInfo> schemaFieldInfos = new ArrayList<>();
        parse(rootSchema, schemaFieldInfos);
        return schemaFieldInfos;
    }

    private void parse(Schema schema, List<SchemaFieldInfo> schemaFieldInfos) {
        if (schema.getType() != Schema.Type.RECORD) {
            LOG.info("Given schema type [{}] is not record", schema.getType());
        } else {
            String fullName = schema.getFullName();
            LOG.debug("Schema full name: [{}]", fullName);

            List<Schema.Field> fields = schema.getFields();
            for (Schema.Field field : fields) {
                parseField(field, schemaFieldInfos);
            }
        }
    }

    private void parseField(Schema.Field field, List<SchemaFieldInfo> schemaFieldInfos) {
        Schema schema = field.schema();
        Schema.Type type = schema.getType();
        String name = field.name();

        LOG.debug("Visiting field: [{}]", field);
        String namespace = null;
        try {
            namespace = schema.getNamespace();
        } catch (Exception e) {
            //ignore.
        }
        schemaFieldInfos.add(new SchemaFieldInfo(namespace, name, type.name()));

        // todo check whether fields should be mapped to the root schema.
        parseSchema(schema, schemaFieldInfos);
    }

    private void parseSchema(Schema schema, List<SchemaFieldInfo> schemaFieldInfos) {
        Schema.Type type = schema.getType();
        LOG.debug("Visiting type: [{}]", type);

        switch (type) {
            case RECORD:
                // store fields of a record.
                List<Schema.Field> fields = schema.getFields();
                for (Schema.Field recordField : fields) {
                    parseField(recordField, schemaFieldInfos);
                }
                break;
            case MAP:
                Schema valueTypeSchema = schema.getValueType();
                parseSchema(valueTypeSchema, schemaFieldInfos);
                break;
            case ENUM:
                break;
            case ARRAY:
                Schema elementType = schema.getElementType();
                parseSchema(elementType, schemaFieldInfos);
                break;

            case UNION:
                List<Schema> unionTypes = schema.getTypes();
                for (Schema typeSchema : unionTypes) {
                    parseSchema(typeSchema, schemaFieldInfos);
                }
                break;

            case STRING:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case FIXED:
            case BOOLEAN:
            case BYTES:
            case NULL:

                break;

            default:
                throw new RuntimeException("Unsupported type: " + type);

        }

    }

}
