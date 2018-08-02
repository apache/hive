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
package org.apache.hadoop.hive.registry;

/**
 * Compatibility across different versions of a given schema
 */
public enum SchemaCompatibility {

    /**
     * No compatibility required between different versions of a schema. Any new schema being added should be semantically
     * valid and no compatibility required with earlier or future schemas.
     */
    NONE,

    /**
     * It indicates that new version of a schema would be compatible with earlier version of that schema. That means
     * the data written from earlier version of the schema, can be deserialized with a new version of the schema.
     */
    BACKWARD,

    /**
     * It indicates that an existing schema is compatible with subsequent versions of the schema. That means the data
     * written from new version of the schema can still be read with old version of the schema.
     */
    FORWARD,

    /**
     * It indicates that a new version of the schema provides both backward and forward compatibilities.
     */
    BOTH;

    public static final SchemaCompatibility DEFAULT_COMPATIBILITY = SchemaCompatibility.BACKWARD;
}
