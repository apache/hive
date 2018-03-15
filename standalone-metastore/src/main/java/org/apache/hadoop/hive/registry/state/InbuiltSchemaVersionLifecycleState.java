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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.registry.errors.IncompatibleSchemaException;
import org.apache.hadoop.hive.registry.errors.SchemaBranchNotFoundException;
import org.apache.hadoop.hive.registry.errors.SchemaNotFoundException;

import java.util.Collection;


public interface InbuiltSchemaVersionLifecycleState extends SchemaVersionLifecycleState {

    default void startReview(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, SchemaNotFoundException {
        throw new SchemaLifecycleException(" This operation is not supported for this instance: " + this);
    }

    default void enable(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        throw new SchemaLifecycleException(" This operation is not supported for this instance: " + this);
    }

    default void disable(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, SchemaNotFoundException {
        throw new SchemaLifecycleException(" This operation is not supported for this instance: " + this);

    }

    default void archive(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, SchemaNotFoundException {
        throw new SchemaLifecycleException(" This operation is not supported for this instance: " + this);
    }

    default void delete(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, SchemaNotFoundException {
        throw new SchemaLifecycleException(" This operation is not supported for this instance: " + this);
    }

    Collection<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> getTransitionActions();

}
