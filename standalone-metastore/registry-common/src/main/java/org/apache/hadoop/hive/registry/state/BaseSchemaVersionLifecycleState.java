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
public class BaseSchemaVersionLifecycleState implements SchemaVersionLifecycleState, Serializable {

    private static final long serialVersionUID = 7503502751825893763L;

    private String name;
    private byte id;
    private String description;

    private BaseSchemaVersionLifecycleState() {
    }

    protected BaseSchemaVersionLifecycleState(String name,
                                              byte id,
                                              String description) {
        this.name = name;
        this.id = id;
        this.description = description;
    }

    @Override
    public Byte getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BaseSchemaVersionLifecycleState that = (BaseSchemaVersionLifecycleState) o;

        if (id != that.id) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return description != null ? description.equals(that.description) : that.description == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (int) id;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BaseSchemaVersionLifecycleState{" +
                "name='" + name + '\'' +
                ", id=" + id +
                ", description='" + description + '\'' +
                '}';
    }
}
