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

package org.apache.hadoop.hive.metastore.registry.client;

import org.apache.hadoop.hive.registry.ISchemaRegistryService;
import org.apache.hadoop.hive.registry.SchemaIdVersion;
import org.apache.hadoop.hive.registry.SerDesInfo;
import org.apache.hadoop.hive.registry.common.errors.IncompatibleSchemaException;
import org.apache.hadoop.hive.registry.common.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.common.errors.SchemaBranchNotFoundException;
import org.apache.hadoop.hive.registry.common.errors.SchemaNotFoundException;
import org.apache.hadoop.hive.registry.serdes.SerDesException;

import java.io.InputStream;

/**
 * This interface defines different methods to interact with remote schema registry.
 * <p>
 * Below code describes how to register new schemas, add new version of a schema and fetch different versions of a schema.
 * <pre>
 *
 * SchemaMetadata  schemaMetadata =  new SchemaMetadata.Builder(name)
 * .type(AvroSchemaProvider.TYPE)
 * .schemaGroup("sample-group")
 * .description("Sample schema")
 * .compatibility(SchemaProvider.Compatibility.BACKWARD)
 * .build();
 *
 * // registering a new schema
 * SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "Initial version of the schema"));
 * LOG.info("Registered schema metadata [{}] and returned version [{}]", schema1, v1);
 *
 * // adding a new version of the schema
 * String schema2 = getSchema("/device-next.avsc");
 * SchemaVersion schemaInfo2 = new SchemaVersion(schema2, "second version");
 * SchemaIdVersion v2 = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);
 * LOG.info("Registered schema metadata [{}] and returned version [{}]", schema2, v2);
 *
 * //adding same schema returns the earlier registered version
 * SchemaIdVersion version = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);
 * LOG.info("Received version [{}] for schema metadata [{}]", version, schemaMetadata);
 *
 * // get a specific version of the schema
 * String schemaName = schemaMetadata.getName();
 * SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v2.getVersion()));
 * LOG.info("Received schema version info [{}] for schema metadata [{}]", schemaVersionInfo, schemaMetadata);
 *
 * // get latest version of the schema
 * SchemaVersionInfo latest = schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);
 * LOG.info("Latest schema with schema key [{}] is : [{}]", schemaMetadata, latest);
 *
 * // get all versions of the schema
 * Collection&lt;SchemaVersionInfo&gt; allVersions = schemaRegistryClient.getAllVersions(schemaName);
 * LOG.info("All versions of schema key [{}] is : [{}]", schemaMetadata, allVersions);
 *
 * // finding schemas containing a specific field
 * SchemaFieldQuery md5FieldQuery = new SchemaFieldQuery.Builder().name("md5").build();
 * Collection&lt;SchemaVersionKey&gt; md5SchemaVersionKeys = schemaRegistryClient.findSchemasByFields(md5FieldQuery);
 * LOG.info("Schemas containing field query [{}] : [{}]", md5FieldQuery, md5SchemaVersionKeys);
 *
 * SchemaFieldQuery txidFieldQuery = new SchemaFieldQuery.Builder().name("txid").build();
 * Collection&lt;SchemaVersionKey&gt; txidSchemaVersionKeys = schemaRegistryClient.findSchemasByFields(txidFieldQuery);
 * LOG.info("Schemas containing field query [{}] : [{}]", txidFieldQuery, txidSchemaVersionKeys);
 *
 *
 * // Default serializer and deserializer for a given schema provider can be retrieved with the below APIs.
 * // for avro,
 * AvroSnapshotSerializer serializer = schemaRegistryClient.getDefaultSerializer(AvroSchemaProvider.TYPE);
 * AvroSnapshotDeserializer deserializer = schemaRegistryClient.getDefaultDeserializer(AvroSchemaProvider.TYPE);
 * </pre>
 * <p>
 * Below code describes how to register serializer and deserializers, map them with a schema etc.
 * <pre>
 * // upload a jar containing serializer and deserializer classes.
 * InputStream inputStream = new FileInputStream("/schema-custom-ser-des.jar");
 * String fileId = schemaRegistryClient.uploadFile(inputStream);
 *
 * // add serializer with the respective uploaded jar file id.
 * SerDesInfo serializerInfo = createSerDesInfo(fileId);
 * Long serDesId = schemaRegistryClient.addSerDes(serializerInfo);
 *
 * // map this serializer with a registered schema
 * schemaRegistryClient.mapSchemaWithSerDes(schemaName, serDesId);
 *
 * // get registered serializers
 * Collection&lt;SerDesInfo&gt; serializers = schemaRegistryClient.getSerializers(schemaName);
 * SerDesInfo registeredSerDesInfo = serializers.iterator().next();
 *
 * //get serializer and serialize the given payload
 * try(AvroSnapshotSerializer snapshotSerializer = schemaRegistryClient.createInstance(registeredSerDesInfo);) {
 * Map&lt;String, Object&gt; config = ...
 * snapshotSerializer.init(config);
 *
 * byte[] serializedData = snapshotSerializer.serialize(input, schemaInfo);
 *
 * </pre>
 */
public interface ISchemaRegistryClient extends ISchemaRegistryService, AutoCloseable {

    /**
     * Uploads the given {@code schemaVersionTextFile} as a new version for the given schema with name {@code schemaName}
     *
     * @param schemaName            name identifying a schema
     * @param description           description about the version of this schema
     * @param schemaVersionTextFile File containing new version of the schema content.
     *
     * @return version of the schema added.
     *
     * @throws InvalidSchemaException      if the given versionedSchema is not valid
     * @throws IncompatibleSchemaException if the given versionedSchema is incompatible according to the compatibility set.
     * @throws SchemaNotFoundException     if the given schemaMetadata not found.
     */
    public SchemaIdVersion uploadSchemaVersion(final String schemaName,
                                               final String description,
                                               final InputStream schemaVersionTextFile)
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException;


    /**
     * Uploads the given {@code schemaVersionTextFile} as a new version for the given schema with name {@code schemaName}
     * @param schemaBranchName      name identifying a schema branch name
     * @param schemaName            name identifying a schema
     * @param description           description about the version of this schema
     * @param schemaVersionTextFile File containing new version of the schema content.
     *
     * @return version of the schema added.
     *
     * @throws InvalidSchemaException      if the given versionedSchema is not valid
     * @throws IncompatibleSchemaException if the given versionedSchema is incompatible according to the compatibility set.
     * @throws SchemaNotFoundException     if the given schemaMetadata not found.
     */
    public SchemaIdVersion uploadSchemaVersion(final String schemaBranchName,
                                               final String schemaName,
                                               final String description,
                                               final InputStream schemaVersionTextFile)
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException;

    /**
     * @param schemaName   name identifying a schema
     * @param toSchemaText text representing the schema to be checked for compatibility
     *
     * @return true if the given {@code toSchemaText} is compatible with all the versions of the schema with name as {@code schemaName}.
     *
     * @throws SchemaNotFoundException if there is no schema metadata registered with the given {@code schemaName}
     *
     */
    boolean isCompatibleWithAllVersions(String schemaName, String toSchemaText) throws SchemaNotFoundException, SchemaBranchNotFoundException ;


    /**
     * @param schemaBranchName  name identifying a schema branch name
     * @param schemaName   name identifying a schema
     * @param toSchemaText text representing the schema to be checked for compatibility
     *
     * @return true if the given {@code toSchemaText} is compatible with all the versions of the schema with name as {@code schemaName}.
     *
     * @throws SchemaNotFoundException if there is no schema metadata registered with the given {@code schemaName}
     *
     */
    boolean isCompatibleWithAllVersions(String schemaBranchName,String schemaName, String toSchemaText) throws SchemaNotFoundException, SchemaBranchNotFoundException ;

    /**
     * Returns a new instance of default serializer configured for the given type of schema.
     *
     * @param type type of the schema like avro.
     * @param <T>  class type of the serializer instance.
     *
     * @return a new instance of default serializer configured
     *
     * @throws SerDesException          if the serializer class is not found or any error while creating an instance of serializer class.
     * @throws IllegalArgumentException if the given {@code type} is not registered as schema provider in the target schema registry.
     */
    public <T> T getDefaultSerializer(String type) throws SerDesException;


    /**
     * @param type type of the schema, For ex: avro.
     * @param <T>  class type of the deserializer instance.
     *
     * @return a new instance of default deserializer configured for given {@code type}
     *
     * @throws SerDesException          if the deserializer class is not found or any error while creating an instance of deserializer class.
     * @throws IllegalArgumentException if the given {@code type} is not registered as schema provider in the target schema registry.
     */
    public <T> T getDefaultDeserializer(String type) throws SerDesException;

    /**
     * Returns a new instance of the respective Serializer class for the given {@code serializerInfo}
     *
     * @param <T>            type of the instance to be created
     * @param serializerInfo serializer information
     *
     * @return new instance of the respective Serializer class
     *
     * @throws SerDesException throws an Exception if serializer or deserializer class is not an instance of {@code T}
     */
    <T> T createSerializerInstance(SerDesInfo serializerInfo);

    /**
     * Returns a new instance of the respective Deserializer class for the given {@code deserializerInfo}
     *
     * @param <T>              type of the instance to be created
     * @param deserializerInfo deserializer information
     *
     * @return new instance of the respective Deserializer class
     *
     * @throws SerDesException throws an Exception if serializer or deserializer class is not an instance of {@code T}
     */
    <T> T createDeserializerInstance(SerDesInfo deserializerInfo);

}