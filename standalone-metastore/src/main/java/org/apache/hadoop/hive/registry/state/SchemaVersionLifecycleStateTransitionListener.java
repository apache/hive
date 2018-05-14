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

/**
 *  This useful to add any custom logic for the existing transitions or any custom transitions registered.
 *  After a SchemaVersionLifecycleStateTransitionListener has been defined, one can register with SchemaVersionLifecycleStateMachine.Builder as shown below
 *  <pre>
 *       builder.getTransitionsWithActions().entrySet().stream().
 *       filter(transitionAction -&lt; transitionAction.getKey().getTargetStateId().equals(SchemaVersionLifecycleStates.ENABLED.getId())).
 *        forEach(transitionAction -&lt; builder.registerListener(transitionAction.getKey(), new SchemaVersionLifecycleStateTransitionListener() {
 *
 *           public void preStateTransition(SchemaVersionLifecycleContext context) {
 *               // preStateTransition() does nothing for this state transition
 *           }
 *
 *           public void postStateTransition(SchemaVersionLifecycleContext context) {
 *               // postStateTransition() calls an external service to notify that a schema version has transitioned to 'ENABLED' state
 *               Long schemaVersionId = context.getSchemaVersionId();
 *               WebTarget webTarget = ClientBuilder.newClient().target(props.get("review.service.url").toString()).
 *               path("/v1/transition/schema/"+schemaVersionId+"/notify");
 *               webTarget.request().post(null);
 *           }
 *       }));
 *  </pre>
 */

public interface SchemaVersionLifecycleStateTransitionListener {

    /**
     *  This method is called before a schema version transitions to the target state
     * @param context  contains contextual information and services required for transition from one state to other state.
     */
    void preStateTransition(final SchemaVersionLifecycleContext context);

    /**
     *  This method is called after a schema version transitions to the target state
     * @param context contains contextual information and services required for transition from one state to other state.
     */
    void postStateTransition(final SchemaVersionLifecycleContext context);
}
