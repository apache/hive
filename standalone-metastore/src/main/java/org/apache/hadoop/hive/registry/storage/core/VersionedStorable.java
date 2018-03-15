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
package org.apache.hadoop.hive.registry.storage.core;

/**
 * Represents a versioned entity which can be stored by storage layer.
 * <p>
 * For example, user wants to create a set of versioned schemas. User creates a root entity containing information
 * about this set of schema like name, compatibility etc. There can be multiple versions of schemas with in that
 * schema set and those can be created with {@link VersionedStorable}.
 */
public interface VersionedStorable extends Storable {

    String ROOT_ENTITY_ID = "rootEntityId";

    String DESCRIPTION = "description";

    String VERSION = "version";


    /**
     * Returns the version of the instance.
     *
     * @return the version
     */
    Integer getVersion();

    /**
     * Returns the description about this version.
     *
     * @return the description
     */
    String getDescription();

    /**
     * Returns entity id for which this version is created.
     * For example, user wants to create a set of versioned schemas. User creates a root entity containing information
     * about this set of schema like name, compatibility etc. There can be multiple versions of schemas with in that
     * schema set then those versions can refer to that schema set entity with rootEntityId.
     *
     * @return the root entity id
     */
    Long getRootEntityId();
}
