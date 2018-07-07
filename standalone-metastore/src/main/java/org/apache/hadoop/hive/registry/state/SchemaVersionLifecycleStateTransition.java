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

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaVersionLifecycleStateTransition implements Serializable {
    private static final long serialVersionUID = -3578936032929400872L;

    private Byte sourceStateId;
    private Byte targetStateId;
    private String name;
    private String description;

    private SchemaVersionLifecycleStateTransition() {
    }

    public SchemaVersionLifecycleStateTransition(Byte sourceStateId, Byte targetStateId) {
        this(sourceStateId, targetStateId, null, null);
    }

    public SchemaVersionLifecycleStateTransition(Byte sourceStateId, Byte targetStateId, String name, String description) {
        this.sourceStateId = sourceStateId;
        this.targetStateId = targetStateId;
        this.name = name;
        this.description = description;
    }

    public Byte getSourceStateId() {
        return sourceStateId;
    }

    public Byte getTargetStateId() {
        return targetStateId;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "Transition{" +
                "sourceStateId=" + sourceStateId +
                ", targetStateId=" + targetStateId +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaVersionLifecycleStateTransition that = (SchemaVersionLifecycleStateTransition) o;

        if (sourceStateId != null ? !sourceStateId.equals(that.sourceStateId) : that.sourceStateId != null)
            return false;
        return targetStateId != null ? targetStateId.equals(that.targetStateId) : that.targetStateId == null;
    }

    @Override
    public int hashCode() {
        int result = sourceStateId != null ? sourceStateId.hashCode() : 0;
        result = 31 * result + (targetStateId != null ? targetStateId.hashCode() : 0);
        return result;
    }

}
