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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

/**
 * This is a representation of {@link SchemaVersionLifecycleStateMachine} which can be serialized/deserialized with
 * json format. This can be used by clients to understand the current schema version lifecycle state and act on
 * accordingly.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class SchemaVersionLifecycleStateMachineInfo implements Serializable {
    private static final long serialVersionUID = -3499349707355937574L;

    private Collection<BaseSchemaVersionLifecycleState> states;
    private Collection<SchemaVersionLifecycleStateTransition> transitions;

    private SchemaVersionLifecycleStateMachineInfo() {
    }

    SchemaVersionLifecycleStateMachineInfo(Collection<SchemaVersionLifecycleState> states,
                                           Collection<SchemaVersionLifecycleStateTransition> transitions) {
        this.states = new ArrayList<>(states.size());
        this.transitions = Lists.newArrayList(transitions);

        for (SchemaVersionLifecycleState state : states) {
            this.states.add(new BaseSchemaVersionLifecycleState(state.getName(), state.getId(), state.getDescription()));
        }
    }

    public Collection<BaseSchemaVersionLifecycleState> getStates() {
        return states;
    }

    public Collection<SchemaVersionLifecycleStateTransition> getTransitions() {
        return transitions;
    }

    @Override
    public String toString() {
        return "Configuration{" +
                "states=" + states +
                ", transitions=" + transitions +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaVersionLifecycleStateMachineInfo that = (SchemaVersionLifecycleStateMachineInfo) o;

        if (states != null ? !states.equals(that.states) : that.states != null) return false;
        return transitions != null ? transitions.equals(that.transitions) : that.transitions == null;
    }

    @Override
    public int hashCode() {
        int result = states != null ? states.hashCode() : 0;
        result = 31 * result + (transitions != null ? transitions.hashCode() : 0);
        return result;
    }

}
