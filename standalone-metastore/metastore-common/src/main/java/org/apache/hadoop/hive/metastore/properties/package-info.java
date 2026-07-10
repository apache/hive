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

/**
 * Type-safe, schema-driven property management for Hive metastore objects (clusters, databases, tables).
 *
 * <p>This package allows declaring, validating, and persisting custom properties with runtime type
 * checking and serialization support.</p>
 *
 * <h2>Core Components</h2>
 *
 * <h3>{@link org.apache.hadoop.hive.metastore.properties.PropertySchema}</h3>
 * <p>Defines the blueprint for what properties can be set on a metastore object.</p>
 * <ul>
 *   <li>Declares allowed properties and their types (STRING, INTEGER, BOOLEAN, DATE, JSON, etc.)</li>
 *   <li>Manages default values for properties</li>
 *   <li>Tracks schema version — incremented when properties are added or removed</li>
 *   <li>Generates a digest (UUID) for schema identity and caching</li>
 * </ul>
 *
 * <h3>{@link org.apache.hadoop.hive.metastore.properties.PropertyType}</h3>
 * <p>Abstract type system for property values with parsing, formatting, and binary serialization.
 * Built-in types: {@code STRING}, {@code BOOLEAN}, {@code INTEGER}, {@code LONG}, {@code DOUBLE},
 * {@code DATETIME} (ISO8601/UTC), {@code JSON} (Gson-backed). Custom types can be registered via
 * {@link org.apache.hadoop.hive.metastore.properties.PropertyType#register(PropertyType)}.</p>
 *
 * <h3>{@link org.apache.hadoop.hive.metastore.properties.PropertyMap}</h3>
 * <p>Holds the actual property values for a specific metastore object instance, validated against
 * a {@code PropertySchema}. Uses copy-on-write semantics gated by a dirty flag for thread-safe,
 * efficient sharing.</p>
 *
 * <h3>{@link org.apache.hadoop.hive.metastore.properties.PropertyManager}</h3>
 * <p>High-level per-session manager that ties schemas into one namespace and drives transactional
 * reads and writes. Tracks dirty maps to batch persistence store updates. Integrates with JEXL
 * for dynamic property expressions.</p>
 *
 * <h3>{@link org.apache.hadoop.hive.metastore.properties.PropertyStore}</h3>
 * <p>Persistence layer for loading and saving property maps to the metastore database.
 * Can be wrapped by {@link org.apache.hadoop.hive.metastore.properties.CachingPropertyStore}
 * for performance.</p>
 *
 * <h2>Workflow Example</h2>
 * <pre>{@code
 * // Define a schema (typically done at startup)
 * PropertySchema tableSchema = new PropertySchema("table-schema", 1, new TreeMap<>());
 * tableSchema.declareProperty("owner", PropertyType.STRING, "admin");
 * tableSchema.declareProperty("is_external", PropertyType.BOOLEAN, false);
 * tableSchema.declareProperty("created_time", PropertyType.DATETIME);
 *
 * // Create a property map for an object, set values, and persist
 * PropertyMap tableProps = manager.newPropertyMap(tableSchema);
 * tableProps.put("owner", "alice");
 * tableProps.put("is_external", true);
 * manager.persistProperties(tableProps);
 * }</pre>
 *
 * <h2>Design Patterns</h2>
 * <ul>
 *   <li><b>Thread safety</b>: schemas use {@code AtomicInteger} version counters; maps use
 *       copy-on-write with dirty flags; digest computation is double-checked locked.</li>
 *   <li><b>Serialization</b>: a custom {@link org.apache.hadoop.hive.metastore.properties.SerializationProxy}
 *       handles transient fields safely; both {@code DataInput}/{@code DataOutput} and Java object
 *       serialization are supported.</li>
 *   <li><b>Schema versioning</b>: version and digest UUID allow detecting schema changes and
 *       support schema evolution without breaking existing data.</li>
 * </ul>
 */
package org.apache.hadoop.hive.metastore.properties;
