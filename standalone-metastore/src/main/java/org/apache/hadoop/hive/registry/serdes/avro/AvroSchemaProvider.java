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

import com.google.common.base.Joiner;
import org.apache.hadoop.hive.registry.AbstractSchemaProvider;
import org.apache.hadoop.hive.registry.CompatibilityResult;
import org.apache.hadoop.hive.registry.SchemaCompatibility;
import org.apache.hadoop.hive.registry.SchemaFieldInfo;
import org.apache.hadoop.hive.registry.common.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.common.errors.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class AvroSchemaProvider extends AbstractSchemaProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaProvider.class);

    public static final String TYPE = "avro";

    @Override
    public String getName() {
        return "Avro schema provider";
    }

    @Override
    public String getDescription() {
        return "This provider supports avro schemas. You can find more information about avro at http://avro.apache.org";
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public CompatibilityResult checkCompatibility(String toSchemaText,
                                                  String existingSchemaText,
                                                  SchemaCompatibility existingSchemaCompatibility) {
        return AvroSchemaValidator
                .of(existingSchemaCompatibility)
                .validate(new Schema.Parser().parse(toSchemaText),
                          new Schema.Parser().parse(existingSchemaText));
    }

    @Override
    public byte[] getFingerprint(String schemaText) throws InvalidSchemaException, SchemaNotFoundException {
        try {
            // generates fingerprint of canonical form of the given schema.
            Schema schema = new Schema.Parser().parse(getResultantSchema(schemaText));
            return MessageDigest.getInstance("MD5").digest(normalize(schema).getBytes());
        } catch (IOException e) {
            throw new InvalidSchemaException("Given schema is invalid", e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getResultantSchema(String schemaText) throws InvalidSchemaException, SchemaNotFoundException {
        AvroSchemaResolver avroSchemaResolver = new AvroSchemaResolver(getSchemaVersionRetriever());
        return avroSchemaResolver.resolveSchema(schemaText);
    }

    @Override
    public List<SchemaFieldInfo> generateFields(String schemaText) throws InvalidSchemaException, SchemaNotFoundException {
        AvroFieldsGenerator avroFieldsGenerator = new AvroFieldsGenerator();
        return avroFieldsGenerator.generateFields(new Schema.Parser().parse(getResultantSchema(schemaText)));
    }

    public String normalize(Schema schema) throws IOException {
        Map<String, String> env = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        return build(env, schema, sb).toString();
    }

    // This is borrowed from avro but this does not fully resolve schema according to resolution rules. For more info
    // https://issues.apache.org/jira/browse/AVRO-2002
    //
    // Added default and aliases handling here.
    private static Appendable build(Map<String, String> env,
                                    Schema schema,
                                    Appendable appendable) throws IOException {
        boolean firstTime = true;
        Schema.Type schemaType = schema.getType();
        String fullName = schema.getFullName();
        switch (schemaType) {
            default: // boolean, bytes, double, float, int, long, null, string
                return appendable.append('"').append(schemaType.getName()).append('"');

            case UNION:
                appendable.append('[');
                for (Schema b : schema.getTypes()) {
                    if (!firstTime) appendable.append(',');
                    else firstTime = false;
                    build(env, b, appendable);
                }
                return appendable.append(']');

            case ARRAY:
            case MAP:
                appendable.append("{\"type\":\"").append(schemaType.getName()).append("\"");
                if (schemaType == Schema.Type.ARRAY)
                    build(env, schema.getElementType(), appendable.append(",\"items\":"));
                else build(env, schema.getValueType(), appendable.append(",\"values\":"));
                return appendable.append("}");

            case ENUM:
                if (env.get(fullName) != null) {
                    return appendable.append(env.get(fullName));
                }
                addNameType(env, appendable, schemaType, fullName);

                appendable.append(",\"symbols\":[");
                for (String enumSymbol : schema.getEnumSymbols()) {
                    if (!firstTime) appendable.append(',');
                    else firstTime = false;
                    appendable.append('"').append(enumSymbol).append('"');
                }
                return appendable.append("]").append("}");

            case FIXED:
                if (env.get(fullName) != null) {
                    return appendable.append(env.get(fullName));
                }
                addNameType(env, appendable, schemaType, fullName);

                return appendable.append(",\"size\":").append(Integer.toString(schema.getFixedSize())).append("}");

            case RECORD:
                if (env.get(fullName) != null) {
                    return appendable.append(env.get(fullName));
                }
                addNameType(env, appendable, schemaType, fullName);

                // avro resolution parsing does not handle aliases and default attributes
                // handle aliases
                Set<String> aliases = schema.getAliases();
                if (aliases != null && !aliases.isEmpty()) {
                    appendable.append("\"aliases\":")
                            .append("[")
                            .append(Joiner.on(",").join(aliases.stream()
                                                                .map(x -> "\"" + x + "\"")
                                                                .collect(Collectors.toList())))
                            .append("]")
                            .append(",");
                }

                appendable.append(",\"fields\":[");
                for (Schema.Field field : schema.getFields()) {
                    if (!firstTime) {
                        appendable.append(',');
                    } else {
                        firstTime = false;
                    }
                    appendable.append("{\"name\":\"").append(field.name()).append("\"").append(",\"type\":");

                    // handle default value
                    JsonNode defaultValue = field.defaultValue();
                    if (defaultValue != null) {
                        appendable.append(defaultValue.toString());
                    }

                    build(env, field.schema(), appendable).append("}");
                }
                return appendable.append("]").append("}");
        }
    }

    private static void addNameType(Map<String, String> env, Appendable appendable, Schema.Type schemaType, String name) throws IOException {
        String qname = "\"" + name + "\"";
        env.put(name, qname);
        appendable.append("{\"name\":").append(qname);
        appendable.append(",\"type\":\"").append(schemaType.getName()).append("\"");
    }

}
