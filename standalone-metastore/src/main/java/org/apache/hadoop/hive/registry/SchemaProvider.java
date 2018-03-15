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

import org.apache.hadoop.hive.registry.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.errors.SchemaNotFoundException;

import java.util.List;
import java.util.Map;

/**
 * SchemaProvider is an abstract way to provide the required functionality for a specific protocol like Avro, Json etc.
 */
public interface SchemaProvider {

    String SCHEMA_VERSION_RETRIEVER_CONFIG = "schemaVersionRetriever";

    /**
     * Initializes with the given {@code config}.
     *
     * @param config configuration of this registry
     */
    public void init(Map<String, Object> config);

    /**
     * @return Name of this provider
     */
    String getName();

    /**
     * @return description about this provider.
     */
    String getDescription();

    /**
     * @return Fully qualified class name of the default serializer.
     */
    String getDefaultSerializerClassName();

    /**
     * @return Fully qualified class name of the default serializer.
     */
    String getDefaultDeserializerClassName();

    /**
     * @return type of this provider. This should be unique among all the registered providers.
     */
    String getType();

    /**
     * Returns CompatibilityResult with {@link CompatibilityResult#isCompatible} as true if the given {@code schemaText}
     * is compatible with all the given {@code existingSchema} according to {@code existingSchemaCompatibility} else
     * CompatibilityResult with {@link CompatibilityResult#isCompatible} as false, {@link CompatibilityResult#getErrorMessage()}
     * with respective errorMessage
     *
     * @param toSchema       schema text to be checked for compatibility.
     * @param existingSchema existing schema to which it is checked for.
     * @param compatibility  compatibility mode
     *
     * @return compatibility result with the given schemas
     */
    CompatibilityResult checkCompatibility(String toSchema, String existingSchema, SchemaCompatibility compatibility);

    /**
     * @param schemaText textual representation of schema
     *
     * @return fingerprint of canonicalized form of the given schema.
     *
     * @throws InvalidSchemaException  when the given schema is invalid.
     * @throws SchemaNotFoundException when there are no schemas found mentioned in include fields of the schema text.
     */
    byte[] getFingerprint(String schemaText) throws InvalidSchemaException, SchemaNotFoundException;

    /**
     * Returns all the fields in the given {@code schemaText} by traversing the whole schema including nested/complex types.
     *
     * @param schemaText schema text
     *
     * @return all the fields in the given {@code schemaText}
     *
     * @throws InvalidSchemaException  when the given schema is invalid.
     * @throws SchemaNotFoundException when there are no schemas found mentioned in include fields of the schema text.
     */
    List<SchemaFieldInfo> generateFields(String schemaText) throws InvalidSchemaException, SchemaNotFoundException;

    /**
     * Returns the resultant schema of the given {@code schemaText} after parsing and replacing any include references
     * with the actual types.
     *
     * @param schemaText schmea text
     *
     * @throws InvalidSchemaException  when the given schemaText does not represent a valid schema
     * @throws SchemaNotFoundException when any of the dependent includedSchemas does not exist
     */
    String getResultantSchema(String schemaText) throws InvalidSchemaException, SchemaNotFoundException;
}
