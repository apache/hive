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

import org.apache.hadoop.hive.registry.common.errors.SchemaNotFoundException;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This is default implementation of CustomSchemaStateExecutor which always leads to the success state.
 */
public class DefaultCustomSchemaStateExecutor implements CustomSchemaStateExecutor {
    private InReviewState inReviewState;

    private SchemaVersionLifecycleState successState;
    private SchemaVersionLifecycleState retryState;

    @Override
    public void init(SchemaVersionLifecycleStateMachine.Builder builder,
                     Byte successStateId,
                     Byte retryStateId,
                     Map<String, ?> props) {
        this.successState = builder.getStates().get(successStateId);
        this.retryState = builder.getStates().get(retryStateId);
        inReviewState = new InReviewState(successState);
        builder.register(inReviewState);

        for (Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction> pair : inReviewState
                .getTransitionActions()) {
            builder.transition(pair.getLeft(), pair.getRight());
        }
    }

    @Override
    public void executeReviewState(SchemaVersionLifecycleContext schemaVersionLifecycleContext)
            throws SchemaLifecycleException, SchemaNotFoundException {
        schemaVersionLifecycleContext.setState(inReviewState);
        schemaVersionLifecycleContext.updateSchemaVersionState();
    }

    private static final class InReviewState extends AbstractInbuiltSchemaLifecycleState {
        private final List<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> transitionActionPair;

        private InReviewState(final SchemaVersionLifecycleState targetState) {
            super("InReview",
                  (byte) 32,
                  "Finish the schema version."
                 );
            SchemaVersionLifecycleStateTransition stateTransition = new SchemaVersionLifecycleStateTransition(getId(), targetState.getId(), "FinishReview", "Finish schema review process");
            SchemaVersionLifecycleStateAction stateAction = context -> {
                context.setState(targetState);
                try {
                    context.updateSchemaVersionState();
                } catch (SchemaNotFoundException e) {
                    throw new SchemaLifecycleException(e);
                }
            };
            transitionActionPair = Collections.singletonList(Pair.of(stateTransition, stateAction));
        }

        @Override
        public Collection<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> getTransitionActions() {
            return transitionActionPair;
        }

        @Override
        public String toString() {
            return "FinishReviewState{" + super.toString() + "}";
        }

    }
}
