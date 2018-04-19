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
package org.apache.hadoop.hive.registry.streams;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.registry.common.Schema;
import org.apache.hadoop.hive.registry.AbstractSchemaProvider;
import org.apache.hadoop.hive.registry.CompatibilityResult;
import org.apache.hadoop.hive.registry.SchemaCompatibility;
import org.apache.hadoop.hive.registry.SchemaFieldInfo;
import org.apache.hadoop.hive.registry.common.errors.InvalidSchemaException;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class StreamsSchemaProvider extends AbstractSchemaProvider {
    public static final String TYPE = "streams";
    public static final String UTF_8 = "UTF-8";
    public static final String MD5 = "MD5";

    public StreamsSchemaProvider() {
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public CompatibilityResult checkCompatibility(String toSchema,
                                                  String existingSchema,
                                                  SchemaCompatibility compatibility) {
        return CompatibilityResult.SUCCESS;
    }

    @Override
    public byte[] getFingerprint(String schemaText) throws InvalidSchemaException {
        try {
            return MessageDigest.getInstance(MD5).digest(schemaText.getBytes(UTF_8));
        } catch (Exception e) {
            throw new InvalidSchemaException(e);
        }
    }

    @Override
    public List<SchemaFieldInfo> generateFields(String schemaText) {
        // schema should be in json form.
        List<Schema.Field> fields;
        try {
            fields = new ObjectMapper().readValue(schemaText, new TypeReference<List<Schema.Field>>() {});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<SchemaFieldInfo> fieldInfos = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            // currently internal schema implementation does not have namespace.
            fieldInfos.add(new SchemaFieldInfo("__universal", field.getName(), field.getType().toString()));
        }

        return fieldInfos;
    }
}
