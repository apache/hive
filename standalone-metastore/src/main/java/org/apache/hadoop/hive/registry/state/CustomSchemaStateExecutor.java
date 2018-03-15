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

import java.util.Map;

/**
 * This interface should be implemented for having custom review process and adding any custom states with the default 
 * state machine. This can be defined in registry.yaml for registration.
 *
 * This is still experimental API and not stable.
 */
public interface CustomSchemaStateExecutor {

    /**
     * Initialize with completion states of this schema review.
     *
     * @param builder        this can be used to add any custom states and transitions.
     * @param successStateId state to be set when review is successful.
     * @param retryStateId   state to be set when review is failed.
     * @param props          any properties to be initialized with.
     */
    void init(SchemaVersionLifecycleStateMachine.Builder builder,
              Byte successStateId,
              Byte retryStateId,
              Map<String, ?> props);

    /**
     * Execute custom review logic for the given {@code schemaVersionLifecycleContext} and update with respective state
     * when it is is successfully executed by using {@link SchemaVersionLifecycleContext#updateSchemaVersionState()}.
     *
     * @param schemaVersionLifecycleContext SchemaVersionLifecycleContext instance
     *
     * @throws SchemaLifecycleException when any lifecycle error occurs.
     * @throws SchemaNotFoundException  when the given schema version is not found.
     */
    void executeReviewState(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, SchemaNotFoundException;
}
