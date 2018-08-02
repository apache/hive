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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.registry.state.SchemaVersionLifecycleState.INBUILT_STATE_ID_MAX;

/**
 * This class represents schema version lifecycle state machine registered in SchemaRegistry server. Users can customize
 * the state by configuring {@link CustomSchemaStateExecutor} and custom states and transitions can be configured using {@link Builder}
 * in callbacks{@link CustomSchemaStateExecutor#init(Builder, Byte, Byte, Map)}
 */
public class SchemaVersionLifecycleStateMachine {

    private final Map<Byte, SchemaVersionLifecycleState> states;
    private final Map<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction> transitions;
    private final Map<SchemaVersionLifecycleStateTransition, List<SchemaVersionLifecycleStateTransitionListener>> listeners;

    private SchemaVersionLifecycleStateMachine(Map<Byte, SchemaVersionLifecycleState> states,
                                               Map<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction> transitions,
                                               Map<SchemaVersionLifecycleStateTransition, ConcurrentLinkedQueue<SchemaVersionLifecycleStateTransitionListener>> listeners) {
        this.states = Collections.unmodifiableMap(states);
        this.transitions = Collections.unmodifiableMap(transitions);
        this.listeners = Collections.unmodifiableMap(listeners).entrySet().stream().
                collect(Collectors.toMap(Map.Entry::getKey, transitionWithListener -> Lists.newArrayList(transitionWithListener.getValue().iterator())));
    }

    public Map<Byte, SchemaVersionLifecycleState> getStates() {
        return states;
    }

    public Map<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction> getTransitions() {
        return transitions;
    }

    public Map<SchemaVersionLifecycleStateTransition, List<SchemaVersionLifecycleStateTransitionListener>> getListeners() {
        return listeners;
    }

    public SchemaVersionLifecycleStateMachineInfo toConfig() {
        return new SchemaVersionLifecycleStateMachineInfo(states.values(), transitions.keySet());
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static class Builder {
        ConcurrentMap<Byte, SchemaVersionLifecycleState> states = new ConcurrentHashMap<>();
        ConcurrentMap<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction> transitionsWithActions = new ConcurrentHashMap<>();
        ConcurrentMap<SchemaVersionLifecycleStateTransition, ConcurrentLinkedQueue<SchemaVersionLifecycleStateTransitionListener>> transitionsWithListeners = new ConcurrentHashMap<>();

        public Builder() {
            registerInBuiltStates();
        }

        private void registerInBuiltStates() {
            List<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> transitionActions = new ArrayList<>();

            Field[] declaredFields = SchemaVersionLifecycleStates.class.getDeclaredFields();
            for (Field field : declaredFields) {
                if (Modifier.isFinal(field.getModifiers()) &&
                        Modifier.isStatic(field.getModifiers()) &&
                        InbuiltSchemaVersionLifecycleState.class.isAssignableFrom(field.getType())) {
                    InbuiltSchemaVersionLifecycleState state = null;
                    try {
                        state = (InbuiltSchemaVersionLifecycleState) field.get(null);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    register(state);

                    transitionActions.addAll(state.getTransitionActions());
                }
            }

            // register transitions for inbuilt states
            for (Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction> transitionAction : transitionActions) {
                transition(transitionAction.getLeft(), transitionAction.getRight());
            }
        }

        /**
         * Registers the given state with REGISTRY.
         *
         * @param state state to be registered.
         *
         * @throws IllegalArgumentException if the given state is already registered.
         */
        public void register(SchemaVersionLifecycleState state) {
            checkForInbuiltStateIds(state);
            SchemaVersionLifecycleState prevState = states.putIfAbsent(state.getId(), state);
            if (prevState != null) {
                throw new IllegalArgumentException("Given state is already registered as " + prevState);
            }
        }

        public Map<Byte, SchemaVersionLifecycleState> getStates() {
            return Collections.unmodifiableMap(states);
        }

        public Builder transition(SchemaVersionLifecycleStateTransition transition,
                                  SchemaVersionLifecycleStateAction action) {
            Byte sourceStateId = transition.getSourceStateId();
            Byte targetStateId = transition.getTargetStateId();
            checkStatesRegistered(sourceStateId, targetStateId);

            SchemaVersionLifecycleStateAction existingTransitionAction = transitionsWithActions.putIfAbsent(transition, action);
            if (existingTransitionAction != null) {
                throw new IllegalArgumentException("Given transition already exists, from: [" + sourceStateId + "] to: [" + targetStateId + "]");
            }

            return this;
        }

        public void registerListener(SchemaVersionLifecycleStateTransition transition, SchemaVersionLifecycleStateTransitionListener listener) {
            SchemaVersionLifecycleStateAction schemaVersionLifecycleStateAction = transitionsWithActions.get(transition);
            if (schemaVersionLifecycleStateAction == null)
                throw new IllegalArgumentException("Given transition doesn't have any action associated with it");
            ConcurrentLinkedQueue<SchemaVersionLifecycleStateTransitionListener> listeners = transitionsWithListeners.computeIfAbsent(transition,
                    missingTransition -> new ConcurrentLinkedQueue<>());
            listeners.add(listener);
            transitionsWithListeners.put(transition, listeners);
        }

        private void checkForInbuiltStateIds(SchemaVersionLifecycleState state) {
            if (!(state instanceof InbuiltSchemaVersionLifecycleState)) {
                if (state.getId() <= INBUILT_STATE_ID_MAX) {
                    throw new IllegalArgumentException("Given custom state id should be more than 32");
                }
            }
        }

        private void checkStatesRegistered(Byte... stateIds) {
            for (Byte stateId : stateIds) {
                if (!this.states.containsKey(stateId)) {
                    throw new IllegalArgumentException("Given state [" + stateId + "] is not yet registered.");
                }
            }
        }

        public Map<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction> getTransitionsWithActions() {
            return Collections.unmodifiableMap(transitionsWithActions);
        }

        public SchemaVersionLifecycleStateMachine build() {
            return new SchemaVersionLifecycleStateMachine(states, transitionsWithActions , transitionsWithListeners);
        }
    }

}
