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

import org.apache.hadoop.hive.registry.errors.IncompatibleSchemaException;
import org.apache.hadoop.hive.registry.errors.InvalidSchemaBranchDeletionException;
import org.apache.hadoop.hive.registry.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.errors.SchemaBranchAlreadyExistsException;
import org.apache.hadoop.hive.registry.errors.SchemaBranchNotFoundException;
import org.apache.hadoop.hive.registry.errors.SchemaNotFoundException;
import org.apache.hadoop.hive.registry.serde.SerDesException;
import org.apache.hadoop.hive.registry.state.SchemaLifecycleException;
import org.apache.hadoop.hive.registry.state.SchemaVersionLifecycleStateMachineInfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;

/**
 * Basic service interface for schema registry which should be implemented by client and server interfaces.
 */
public interface ISchemaRegistryService {

    /**
     * @return Collection of supported schema providers. For ex: avro.
     */
    Collection<SchemaProviderInfo> getSupportedSchemaProviders();

    /**
     * Registers information about a schema.
     *
     * @param schemaMetadata metadata about schema.
     *
     * @return id of the registered schema which is successfully registered now or earlier.
     *
     * @deprecated This APi is deprecated since 0.3.0, You can use {@link #addSchemaMetadata(SchemaMetadata)} for the same.
     */
    Long registerSchemaMetadata(SchemaMetadata schemaMetadata);


    /**
     * Registers information about a schema.
     *
     * @param schemaMetadata metadata about schema.
     *
     * @return id of the registered schema which is successfully registered now or earlier.
     */
    Long addSchemaMetadata(SchemaMetadata schemaMetadata);

    /**
     * Updates information about a schema.
     *
     * @param schemaName     Schema name for which the metadata is updated.
     * @param schemaMetadata information about schema.
     *
     * @return information about given schema identified by {@code schemaName} after update.
     */
    SchemaMetadataInfo updateSchemaMetadata(String schemaName, SchemaMetadata schemaMetadata);

    /**
     * @param schemaName name identifying a schema
     *
     * @return information about given schema identified by {@code schemaName}
     */
    SchemaMetadataInfo getSchemaMetadataInfo(String schemaName);


    /**
     * @param schemaMetadataId id of schema metadata
     *
     * @return information about given schema identified by {@code schemaMetadataId}
     */
    SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId);

    /**
     * Returns version of the schema added with the given schemaInfo.
     * <pre>
     * It tries to fetch an existing schema or register the given schema with the below conditions
     *  - Checks whether there exists a schema with the given name and schemaText
     *      - returns respective schemaVersionKey if it exists.
     *      - Creates a schema for the given name and returns respective schemaVersionKey if it does not exist.
     * </pre>
     *
     * @param schemaMetadata information about the schema
     * @param schemaVersion  new version of the schema to be registered
     *
     * @return version of the schema added.
     *
     * @throws InvalidSchemaException      if the given versionedSchema is not valid
     * @throws IncompatibleSchemaException if the given versionedSchema is incompatible according to the compatibility set.
     * @throws SchemaNotFoundException     if the given schemaMetadata not found.
     */
    SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata,
                                     SchemaVersion schemaVersion) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException;


    /**
     * Returns version of the schema added with the given schemaInfo.
     * <pre>
     * It tries to fetch an existing schema or register the given schema with the below conditions
     *  - Checks whether there exists a schema with the given name and schemaText
     *      - returns respective schemaVersionKey if it exists.
     *      - Creates a schema for the given name and returns respective schemaVersionKey if it does not exist.
     * </pre>
     *
     * @param schemaBranchName name of the schema branch
     * @param schemaMetadata information about the schema
     * @param schemaVersion  new version of the schema to be registered
     *
     * @return version of the schema added.
     *
     * @throws InvalidSchemaException      if the given versionedSchema is not valid
     * @throws IncompatibleSchemaException if the given versionedSchema is incompatible according to the compatibility set.
     * @throws SchemaNotFoundException     if the given schemaMetadata not found.
     */
    SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                     SchemaMetadata schemaMetadata,
                                     SchemaVersion schemaVersion) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException;

    /**
     * Adds the given {@code schemaVersion} and returns the corresponding version number.
     *
     * @param schemaName    name identifying a schema
     * @param schemaVersion new version of the schema to be added
     *
     * @return version number of the schema added
     *
     * @throws InvalidSchemaException      if the given schemaVersion is not valid
     * @throws IncompatibleSchemaException if the given schemaVersion is incompatible according to the compatibility set.
     * @throws SchemaNotFoundException     if there is no schema metadata registered with the given {@code schemaName}
     */
    SchemaIdVersion addSchemaVersion(String schemaName,
                                     SchemaVersion schemaVersion) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException;


    /**
     * Adds the given {@code schemaVersion} and returns the corresponding version number.
     *
     * @param schemaBranchName name of the schema branch
     * @param schemaName    name identifying a schema
     * @param schemaVersion new version of the schema to be added
     *
     * @return version number of the schema added
     *
     * @throws InvalidSchemaException      if the given schemaVersion is not valid
     * @throws IncompatibleSchemaException if the given schemaVersion is incompatible according to the compatibility set.
     * @throws SchemaNotFoundException     if there is no schema metadata registered with the given {@code schemaName}
     */
    SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                     String schemaName,
                                     SchemaVersion schemaVersion) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException;
    /**
     * Deletes a schema version given {@code schemaVersionKey}, throws an SchemaNotFoundException if the schema version is absent.
     *
     * @param schemaVersionKey key identifying a schema and a version
     *
     * @throws SchemaNotFoundException when there is no schema version exists with the given {@code schemaVersionKey}
     */
    void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException, SchemaLifecycleException;

    /**
     * @param schemaVersionKey key identifying a schema and a version
     *
     * @return {@link SchemaVersionInfo} for the given {@link SchemaVersionKey}
     *
     * @throws SchemaNotFoundException when there is no schema version exists with the given {@code schemaVersionKey}
     */
    SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException;

    /**
     * @param schemaIdVersion key identifying a schema and a version
     *
     * @return {@link SchemaVersionInfo} for the given {@link SchemaIdVersion}
     *
     * @throws SchemaNotFoundException when there is no schema version exists with the given {@code schemaIdVersion}
     */
    SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException;

    /**
     * @param schemaName name identifying a schema
     *
     * @return latest version of the schema for the given schemaName
     *
     * @throws SchemaNotFoundException if there is no schema metadata registered with the given {@code schemaName}
     */
    SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException;

    /**
     * @param schemaBranchName name of the schema branch
     * @param schemaName name identifying a schema
     *
     * @return latest version of the schema for the given schemaName
     *
     * @throws SchemaNotFoundException if there is no schema metadata registered with the given {@code schemaName}
     */
    SchemaVersionInfo getLatestSchemaVersionInfo(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException;

    /**
     * @param schemaName name identifying a schema
     *
     * @return all versions of the schemas for given schemaName
     *
     * @throws SchemaNotFoundException if there is no schema metadata registered with the given {@code schemaName}
     */
    Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException;

    /**
     * @param schemaBranchName name of the schema branch
     * @param schemaName name identifying a schema
     *
     * @return all versions of the schemas for given schemaName
     *
     * @throws SchemaNotFoundException if there is no schema metadata registered with the given {@code schemaName}
     */
    Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException;

    /**
     * @param schemaName   name identifying a schema
     * @param toSchemaText text representing the schema to be checked for compatibility
     *
     * @return true if the given {@code toSchemaText} is compatible with the validation level of the schema with id as {@code schemaName}.
     *
     * @throws SchemaNotFoundException if there is no schema metadata registered with the given {@code schemaName}
     */
    CompatibilityResult checkCompatibility(String schemaName, String toSchemaText) throws SchemaNotFoundException, SchemaBranchNotFoundException;


    /**
     * @param schemaBranchName name of the schema branch
     * @param schemaName   name identifying a schema
     * @param toSchemaText text representing the schema to be checked for compatibility
     *
     * @return true if the given {@code toSchemaText} is compatible with the validation level of the schema with id as {@code schemaName}.
     *
     * @throws SchemaNotFoundException if there is no schema metadata registered with the given {@code schemaName}
     */
    CompatibilityResult checkCompatibility(String schemaBranchName, String schemaName, String toSchemaText) throws SchemaNotFoundException, SchemaBranchNotFoundException;

    /**
     * @param schemaFieldQuery {@link SchemaFieldQuery} instance to be run
     *
     * @return schema versions matching the fields specified in the query
     */
    Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) throws SchemaBranchNotFoundException, SchemaNotFoundException;

    /**
     * Uploads the given {@code inputStream} of any file and returns the identifier for which it can be downloaded later
     * with {@link #downloadFile(String)}.
     *
     * @param inputStream input stream of a file to be uploaded.
     *
     * @return unique id for the uploaded bytes read from input stream to file storage.
     *
     * @throws SerDesException if any error occurs while this operation is being done.
     */
    String uploadFile(InputStream inputStream) throws SerDesException;

    /**
     * Downloads the content of file stored with the given {@code fileId} earlier uploaded using {@link #uploadFile(InputStream)}.
     *
     * @param fileId file identifier
     *
     * @return {@link InputStream} instance of the file stored earlier with {@code fileId}
     *
     * @throws IOException when there is no file stored with the given {@code fileId}
     */
    InputStream downloadFile(String fileId) throws IOException;

    /**
     * @param serializerInfo serializer information
     *
     * @return unique id for the added Serializer for the given {@code serializerInfo}
     */
    Long addSerDes(SerDesPair serializerInfo);

    /**
     * Maps Serializer/Deserializer of the given {@code serDesId} to Schema with {@code schemaName}
     *
     * @param schemaName name identifying a schema
     * @param serDesId   serializer/deserializer
     */
    void mapSchemaWithSerDes(String schemaName, Long serDesId);

    /**
     * @param schemaName name identifying a schema
     *
     * @return Collection of Serializers registered for the schema with {@code schemaName}
     */
    Collection<SerDesInfo> getSerDes(String schemaName);

    default void enableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException, SchemaBranchNotFoundException {
        throw new UnsupportedOperationException();
    }

    default void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        throw new UnsupportedOperationException();
    }

    default void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        throw new UnsupportedOperationException();
    }

    default void disableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        throw new UnsupportedOperationException();
    }

    default void startSchemaVersionReview(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        throw new UnsupportedOperationException();
    }

    /**
     *  Merge a schema version to 'MASTER' branch
     * @param schemaVersionId  schema version to be used to merge to 'MASTER'
     * @return
     * @throws SchemaNotFoundException  if the {@code schemaVersionId} is can't found
     * @throws IncompatibleSchemaException  if the {@code schemaVersionId} to be merged is not compatible with the schema versions for that schema metadata in 'MASTER' branch
     */

    default SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, IncompatibleSchemaException {
        throw new UnsupportedOperationException();
    }

    void transitionState(Long schemaVersionId, Byte targetStateId, byte[] transitionDetails) throws SchemaNotFoundException, SchemaLifecycleException;

    SchemaVersionLifecycleStateMachineInfo getSchemaVersionLifecycleStateMachineInfo();

    /**
     *  Create a schema branch from a schema version id
     * @param schemaVersionId    schema version id to be used for as the initial version for schema branch to be created
     * @param schemaBranch       schema branch object to be used for creating a schema branch
     * @return
     * @throws SchemaBranchAlreadyExistsException {@code schemaBranch} already exists
     * @throws SchemaNotFoundException  {@code schemaVersionId} to be used to create a {@code schemaBranch} can't be found
     */
    SchemaBranch createSchemaBranch(Long schemaVersionId, SchemaBranch schemaBranch) throws SchemaBranchAlreadyExistsException, SchemaNotFoundException;

    Collection<SchemaBranch> getSchemaBranches(String schemaName) throws SchemaNotFoundException;

    /**
     *  Delete a schema branch and all the schema version part of the branch, only if the schema versions are not in enabled state.
     * @param schemaBranchId  ID of the schema branch
     * @throws SchemaBranchNotFoundException  if the schema branch to be deleted does not exists
     * @throws InvalidSchemaBranchDeletionException  if the schema branch can't be deleted at the moment
     */

    void deleteSchemaBranch(Long schemaBranchId) throws SchemaBranchNotFoundException, InvalidSchemaBranchDeletionException;


    /**
     * Get all the schema metadata with given schemaName and in one of the state ids
     * @param schemaBranchName name of the schema branch
     * @param schemaName name identifying a schema
     * @param stateIds state ids of the schema version
     *
     * @return all versions of the schemas for given schemaName and in one of state ids
     *
     * @throws SchemaNotFoundException if there is no schema metadata registered with the given {@code schemaName}
     */
    Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName, List<Byte> stateIds) throws SchemaNotFoundException, SchemaBranchNotFoundException;
}
