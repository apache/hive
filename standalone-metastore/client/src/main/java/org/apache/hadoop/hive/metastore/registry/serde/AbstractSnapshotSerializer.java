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
package org.apache.hadoop.hive.metastore.registry.serde;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import org.apache.hadoop.hive.metastore.registry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.hadoop.hive.metastore.registry.exceptions.RegistryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements {@link SnapshotSerializer} and internally creates schema registry client to connect to the
 * target schema registry.
 *
 * Extensions of this class need to implement below methods.
 * <ul>
 *    <li>{@link #doSerialize(Object, SchemaIdVersion)}</li>
 *    <li>{@link #getSchemaText(Object)}</li>
 * </ul>
 */
public abstract class AbstractSnapshotSerializer<I, O> extends AbstractSerDes implements SnapshotSerializer<I, O, SchemaMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSnapshotSerializer.class);

    public AbstractSnapshotSerializer() {
    }

    public AbstractSnapshotSerializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }

    @Override
    public final O serialize(I input, SchemaMetadata schemaMetadata) throws SerDesException {
        if(!initialized) {
            throw new IllegalStateException("init should be invoked before invoking serialize operation");
        }
        if(closed) {
            throw new IllegalStateException("This serializer is already closed");
        }

        // compute schema based on input object
        String schema = getSchemaText(input);

        // register that schema and get the version
        try {
            SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema, "Schema registered by serializer:" + this.getClass()));
            // write the version and given object to the output
            return doSerialize(input, schemaIdVersion);
        } catch (SchemaNotFoundException | IncompatibleSchemaException | InvalidSchemaException | SchemaBranchNotFoundException e) {
            throw new RegistryException(e);
        }
    }

    /**
     * Returns textual representation of the schema for the given {@code input} payload.
     * @param input input payload
     */
    protected abstract String getSchemaText(I input);

    /**
     * Returns the serialized object (which can be byte array or inputstream or any other object) which may contain all
     * the required information for deserializer to deserialize into the given {@code input}.
     *
     * @param input input object to be serialized
     * @param schemaIdVersion schema version info of the given input
     * @throws SerDesException when any ser/des Exception occurs
     */
    protected abstract O doSerialize(I input, SchemaIdVersion schemaIdVersion) throws SerDesException;

}
