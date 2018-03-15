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
package org.apache.hadoop.hive.registry.common;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collection;

/**
 * A wrapper entity for passing collection (more than one resource) back to the client.
 * This response is used only for succeed requests.
 * <p>
 * This can be expanded to handle paged result.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CollectionResponse {
    /**
     * For response that returns a collection of entities.
     */
    private Collection<?> entities;

    private CollectionResponse() {}

    public void setEntities(Collection<?> entities) {
        this.entities = entities;
    }

    public Collection<?> getEntities() {
        return entities;
    }

    public static Builder newResponse() {
        return new Builder();
    }

    public static class Builder {
        private Collection<?> entities;

        private Builder() {
        }

        public CollectionResponse.Builder entities(Collection<?> entities) {
            this.entities = entities;
            return this;
        }

        public CollectionResponse build() {
            CollectionResponse response = new CollectionResponse();
            response.setEntities(entities);
            return response;
        }
    }
}

