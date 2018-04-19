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

import org.apache.hadoop.hive.registry.common.errors.IncompatibleSchemaException;
import org.apache.hadoop.hive.registry.common.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.common.errors.SchemaBranchNotFoundException;
import org.apache.hadoop.hive.registry.common.errors.SchemaNotFoundException;
import org.apache.hadoop.hive.registry.common.search.OrderBy;
import org.apache.hadoop.hive.registry.common.search.WhereClause;

import java.util.Collection;
import java.util.List;
import java.util.Map;


public interface ISchemaRegistry extends ISchemaRegistryService {

    String SCHEMA_PROVIDERS = "schemaProviders";

    /**
     * initializes it with the given properties
     *
     * @param props properties to be initialized with.
     */
    void init(Map<String, Object> props);

    /**
     * Registers information about a schema if it is not yet and returns it's identifier.
     *
     * @param schemaMetadata     metadata about schema.
     * @param throwErrorIfExists whether to throw an error if it already exists.
     *
     * @return id of the registered schema which is successfully registered now or earlier.
     */
    Long addSchemaMetadata(SchemaMetadata schemaMetadata, boolean throwErrorIfExists);

    /**
     * If there is a version of the schema with the given schemaText for schema name then it returns respective {@link SchemaVersionInfo},
     * else it returns null.
     *
     * @param schemaName name of the schema
     * @param schemaText text of the schema
     *
     * @return SchemaVersionInfo instance about the registered version of schema which is same as the given {@code schemaText}
     *
     * @throws SchemaNotFoundException when no schema metadata registered with the given schema name.
     * @throws InvalidSchemaException  when the given {@code schemaText} is not valid.
     */
    SchemaVersionInfo getSchemaVersionInfo(String schemaName, String schemaText) throws SchemaNotFoundException, InvalidSchemaException, SchemaBranchNotFoundException;

    /**
     * @param props properties
     *
     * @return Collects aggregated schema metadata which contains the given properties.
     */
    //Collection<AggregatedSchemaMetadataInfo> findAggregatedSchemaMetadata(Map<String, String> props) throws SchemaNotFoundException, SchemaBranchNotFoundException;

    /**
     * @param schemaName name of the schema
     *
     * @return {@link AggregatedSchemaMetadataInfo} for the given schema name, null if there is no schema registered with
     * the given schema name.
     */
    //AggregatedSchemaMetadataInfo getAggregatedSchemaMetadataInfo(String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException;

    /**
     * @param props properties
     *
     * @return All SchemaMetadata having the given properties.
     */
    Collection<SchemaMetadataInfo> findSchemaMetadata(Map<String, String> props);

    /**
     * @param serDesId id
     *
     * @return SerDesInfo for the given serDesId, null if it does not exist.
     */
    //SerDesInfo getSerDes(Long serDesId);

    /**
     * Searches the registry to find schemas according to the given {@code whereClause} and orders the results by given {@code orderByFields}
     *
     * @param whereClause
     * @param orderByFields
     *
     * @return Collection of schemas from the results of given where clause.
     */
    //Collection<SchemaMetadataInfo> searchSchemas(WhereClause whereClause, List<OrderBy> orderByFields);

    /**
     *  Merges a given schema version to 'MASTER' branch with a merge strategy
     * @param schemaVersionId             id of the schema version to be merged
     * @param schemaVersionMergeStrategy  merge strategy to be used for merging to 'MASTER'
     * @return
     * @throws SchemaNotFoundException
     * @throws IncompatibleSchemaException
     */
    default SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId, SchemaVersionMergeStrategy schemaVersionMergeStrategy) throws SchemaNotFoundException, IncompatibleSchemaException {
        throw new UnsupportedOperationException();
    }


    /**
     * @param schemaName name identifying a schema
     *
     * @return all schema branches with versions of the schemas for given schemaName
     *
     * @throws SchemaNotFoundException if there is no schema metadata registered with the given {@code schemaName}
     */
    Collection<AggregatedSchemaBranch> getAggregatedSchemaBranch(String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException;


}