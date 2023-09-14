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

package org.apache.hadoop.hive.metastore.properties;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A property manager tailored for the HiveMetaStore.
 * It describes properties for cluster, database and table based on declared schemas.
 * A property is of the form:
 * <ul>
 *   <li>name : when it refers to a cluster property named 'name'</li>
 *   <li>db.name : when it refers to a database property named 'name' for the database 'db'</li>
 *   <li>db.table.name : when it refers to a table property named 'name' for the table 'table' in the database 'db</li>
 * </ul>
 */
public class HMSPropertyManager extends PropertyManager {
  private static final  String CLUSTER_PREFIX = "cluster";
  private static final String DATABASE_PREFIX = "database";
  private static final String TABLE_PREFIX = "table";
  /* Declare HMS. */
  static {
    PropertyManager.declare("hms", HMSPropertyManager.class);
  }

  /** Table maintenance operation type. */
  public enum MaintenanceOpType {
    COMPACTION,
    SNAPSHOT_EXPIRY,
    STATS_REBUILD,
    MV_BUILD,
    MV_REFRESH,
    SHUFFLE_TO_NEW_PART,
    RECOMPRESS,
    REORG
  }

  /**
   * Finds an op-type by its ordinal.
   * @param ordinal the enum ordinal
   * @return the enum value or null if not found
   */
  public static MaintenanceOpType findOpType(int ordinal) {
    return MOP.get(ordinal);
  }

  /** The map form ordinal to OpType. */
  private static final Map<Integer, MaintenanceOpType> MOP;
  static {
    MOP = new HashMap<>(MaintenanceOpType.values().length);
    Arrays.stream(MaintenanceOpType.values()).forEach(e -> MOP.put(e.ordinal(), e));
  }

  /** Table maintenance operation status. */
  public enum MaintenanceOpStatus {
    MAINTENANCE_NEEDED,
    SCHEDULED,
    IN_PROGRESS,
    DONE,
    CLEANUP_NEEDED,
    FAILED
  }
  /** The map form ordinal to OpStatus. */
  private static final Map<Integer, MaintenanceOpStatus> MOS;
  static {
    MOS = new HashMap<>(MaintenanceOpStatus.values().length);
    Arrays.stream(MaintenanceOpStatus.values()).forEach(e -> MOS.put(e.ordinal(), e));
  }
  /**
   * Finds an op-type by its ordinal.
   * @param ordinal the enum ordinal
   * @return the enum value or null if not found
   */
  public static MaintenanceOpStatus findOpStatus(int ordinal) {
    return MOS.get(ordinal);
  }

  /**
   * Maintenance Operation Type.
   */
  public static final PropertyType<MaintenanceOpType> MAINTENANCE_OPERATION = new PropertyType<MaintenanceOpType>("MaintenanceOperation"){
    @Override public MaintenanceOpType cast(Object value) {
      if (value instanceof MaintenanceOpType) {
        return (MaintenanceOpType) value;
      }
      if (value == null) {
        return null;
      }
      if (value instanceof Number) {
        return findOpType(((Number) value).intValue());
      }
      return parse(value.toString());
    }
    @Override public MaintenanceOpType parse(String str) {
      if (str == null) {
        return null;
      }
      return MaintenanceOpType.valueOf(str.toUpperCase());
    }

    @Override public String format(Object value) {
      if (value instanceof MaintenanceOpType) {
        return value.toString();
      }
      return null;
    }
  };

  /**
   * Maintenance Operation Status.
   */
  public static final PropertyType<MaintenanceOpStatus> MAINTENANCE_STATUS = new PropertyType<MaintenanceOpStatus>("MaintenanceStatus"){
    @Override public MaintenanceOpStatus cast(Object value) {
      if (value instanceof MaintenanceOpStatus) {
        return (MaintenanceOpStatus) value;
      }
      if (value == null) {
        return null;
      }
      if (value instanceof Number) {
        return findOpStatus(((Number) value).intValue());
      }
      return parse(value.toString());
    }
    @Override public MaintenanceOpStatus parse(String str) {
      if (str == null) {
        return null;
      }
      return MaintenanceOpStatus.valueOf(str.toUpperCase());
    }
    @Override public String format(Object value) {
      if (value instanceof MaintenanceOpStatus) {
        return value.toString();
      }
      return null;
    }
  };

  static {
    PropertyType.register(MAINTENANCE_OPERATION);
    PropertyType.register(MAINTENANCE_STATUS);
  }

  /**
   * Creates manager from a property store instance.
   * @param store the store
   */
  public HMSPropertyManager(String ns, PropertyStore store) {
    super(ns, Objects.requireNonNull(store));
  }

  public HMSPropertyManager(PropertyStore store) {
    this("hms", store);
  }

  /**
   * The cluster declared properties.
   *
   */
  public static final PropertySchema CLUSTER_SCHEMA;
  static {
    Map<String, PropertyType<?>> clusterp = new TreeMap<>();
    CLUSTER_SCHEMA = new PropertySchema(CLUSTER_PREFIX, 1, clusterp);
  }

  /**
   * The database declared properties.
   */
  public static final PropertySchema DATABASE_SCHEMA;
  static {
    Map<String,PropertyType<?>> databasep = new TreeMap<>();
    DATABASE_SCHEMA = new PropertySchema(DATABASE_PREFIX, 1, databasep);
  }

  /**
   * The table declared properties.
   */
  public static final PropertySchema TABLE_SCHEMA;
  static {
    Map<String,PropertyType<?>> tablep = new TreeMap<>();
    TABLE_SCHEMA = new PropertySchema(TABLE_PREFIX, 1, tablep);
  }

  /**
   * The various schemas in the DLM.
   */
  private static final PropertySchema[] SCHEMAS = new PropertySchema[]{CLUSTER_SCHEMA, DATABASE_SCHEMA, TABLE_SCHEMA};

  /**
   * Declares a new cluster property.
   * @param name the property name
   * @param type the property type
   * @param defaultValue the property default value or null if none
   */
  public static void declareClusterProperty(String name, PropertyType<?> type, Object defaultValue) {
    CLUSTER_SCHEMA.declareProperty(name, type, defaultValue);
  }

  /**
   * Declares a new database property.
   * @param name the property name
   * @param type the property type
   * @param defaultValue the property default value or null if none
   */
  public static void declareDatabaseProperty(String name, PropertyType<?> type, Object defaultValue) {
    DATABASE_SCHEMA.declareProperty(name, type, defaultValue);
  }

  /**
   * Declares a new table property.
   * @param name the property name
   * @param type the property type
   * @param defaultValue the property default value or null if none
   */
  public static void declareTableProperty(String name, PropertyType<?> type, Object defaultValue) {
    TABLE_SCHEMA.declareProperty(name, type, defaultValue);
  }

  /**
   * The property map factory.
   * @param schemaName the map type (cluster, database, table)
   * @return a property schema instance
   */
  @Override
  public PropertySchema getSchema(String schemaName) {
    switch(schemaName) {
      case CLUSTER_PREFIX: return CLUSTER_SCHEMA;
      case DATABASE_PREFIX: return DATABASE_SCHEMA;
      case TABLE_PREFIX: return TABLE_SCHEMA;
      default: return null;
    }
  }

  /**
   * Determines the schema from the property name fragments.
   * <p>The number of fragments is enough to determine if we are dealing with cluster (1 fragment, the
   * property name), database (2 fragments, 1 for db name, 2 for property), table (3 fragments, db name;,
   * table name, property name).</p>
   * @param keys the property key fragments
   * @return the schema
   */
  @Override protected PropertySchema schemaOf(String[] keys) {
    return keys.length > 0 && keys.length < SCHEMAS.length? SCHEMAS[keys.length - 1] : TABLE_SCHEMA;
  }

  /**
   * The number of fragments that will be used to determine the map name.
   * @param keys key fragments
   * @return a number equal (or inferior) to <code>keys.length - 1</code> since a property name is at least 1 fragment
   */
  @Override protected int getMapNameLength(String[] keys) {
    return SCHEMAS.length;
  }
}
