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
package org.apache.hadoop.hive.registry.state;

import org.apache.hadoop.hive.registry.errors.SchemaNotFoundException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class contains contextual information and services required for transition from one state to other state.
 */
public class SchemaVersionLifecycleContext {
    private SchemaVersionLifecycleState state;
    private Long schemaVersionId;
    private Integer sequence;
    private SchemaVersionService schemaVersionService;
    private SchemaVersionLifecycleStateMachine schemaVersionLifecycleStateMachine;
    private CustomSchemaStateExecutor customSchemaStateExecutor;
    private byte[] stateDetails;
    private ConcurrentMap<String, Object> props = new ConcurrentHashMap<>();

    public SchemaVersionLifecycleContext(Long schemaVersionId,
                                         Integer sequence,
                                         SchemaVersionService schemaVersionService,
                                         SchemaVersionLifecycleStateMachine schemaVersionLifecycleStateMachine,
                                         CustomSchemaStateExecutor customSchemaStateExecutor) {
        this.schemaVersionId = schemaVersionId;
        this.sequence = sequence;
        this.schemaVersionService = schemaVersionService;
        this.schemaVersionLifecycleStateMachine = schemaVersionLifecycleStateMachine;
        this.customSchemaStateExecutor = customSchemaStateExecutor;
    }

    /**
     * Sets the given state as target state for the given schema version identified by {@code schemaVersionId}.
     *
     * @param state target state.
     */
    public void setState(SchemaVersionLifecycleState state) {
        this.state = state;
    }

    /**
     * @return Target state to be set.
     */
    public SchemaVersionLifecycleState getState() {
        return state;
    }

    /**
     * @return schema version id for which state change has to happen.
     */
    public Long getSchemaVersionId() {
        return schemaVersionId;
    }

    /**
     * @return Current sequence of the schema version state updation.
     */
    public Integer getSequence() {
        return sequence;
    }

    /**
     * @return {@link SchemaVersionService} which can be used to have different operations with schema versions etc.
     */
    public SchemaVersionService getSchemaVersionService() {
        return schemaVersionService;
    }

    public CustomSchemaStateExecutor getCustomSchemaStateExecutor() {
        return customSchemaStateExecutor;
    }

    /**
     * @return SchemaVersionLifecycleStateMachine registered for this SchemaRegistry instance.
     */
    public SchemaVersionLifecycleStateMachine getSchemaLifeCycleStatesMachine() {
        return schemaVersionLifecycleStateMachine;
    }

    /**
     * @param key key object
     * @return any property registered for the given key.
     */
    public Object getProperty(String key) {
        return props.get(key);
    }

    /**
     * Set the given property value for the given property key.
     *
     * @param key key object
     * @param value value object
     * @return Any value that is already registered for the given key.
     */
    public Object setProperty(String key, String value) {
        return props.put(key, value);
    }

    /**
     * Updates the current context into the schema version state storage.
     *
     * @throws SchemaLifecycleException when any error occurs while updating the state.
     * @throws SchemaNotFoundException when there is no schema/version found with the given {@code schemaVersionId}.
     */
    public void updateSchemaVersionState() throws SchemaLifecycleException, SchemaNotFoundException {
        schemaVersionService.updateSchemaVersionState(this);
    }

    public void setDetails(byte [] stateDetails) {
        this.stateDetails = stateDetails;
    }

    public byte [] getDetails() {
        return stateDetails;
    }

    @Override
    public String toString() {
        return "SchemaVersionLifecycleContext{" +
                "state=" + state +
                ", schemaVersionId=" + schemaVersionId +
                ", sequence=" + sequence +
                ", details=" + stateDetails +
                ", schemaVersionService=" + schemaVersionService +
                ", schemaVersionLifecycleStateMachine=" + schemaVersionLifecycleStateMachine +
                ", props=" + props +
                '}';
    }
}
