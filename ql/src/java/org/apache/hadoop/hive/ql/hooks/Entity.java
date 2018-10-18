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

package org.apache.hadoop.hive.ql.hooks;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;

/**
 * This class encapsulates an object that is being read or written to by the
 * query. This object may be a table, partition, dfs directory or a local
 * directory.
 */
public class Entity implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * The type of the entity.
   */
  public static enum Type {
    DATABASE, TABLE, PARTITION, DUMMYPARTITION, DFS_DIR, LOCAL_DIR, FUNCTION, SERVICE_NAME
  }

  /**
   * The database if this is a database.
   */
  private Database database;

  /**
   * The type.
   */
  private Type typ;

  /**
   * The table. This is null if this is a directory.
   */
  private Table t;

  /**
   * The partition.This is null if this object is not a partition.
   */
  private Partition p;

  /**
   * The directory if this is a directory
   */
  private Path d;

  /**
   * An object that is represented as a String
   * Currently used for functions and service name
   */
  private String stringObject;

  /**
   * The class name for a function
   */
  private String className;

  /**
   * This is derived from t and p, but we need to serialize this field to make
   * sure Entity.hashCode() does not need to recursively read into t and p.
   */
  private final String name;

  /**
   * Whether the output is complete or not. For eg, for dynamic partitions, the
   * complete output may not be known
   */
  private boolean complete;

  public boolean isComplete() {
    return complete;
  }

  public void setComplete(boolean complete) {
    this.complete = complete;
  }

  public String getName() {
    return name;
  }

  public Database getDatabase() {
    return database;
  }

  public void setDatabase(Database database) {
    this.database = database;
  }

  public Type getTyp() {
    return typ;
  }

  public void setTyp(Type typ) {
    this.typ = typ;
  }

  public Table getT() {
    return t;
  }

  public void setT(Table t) {
    this.t = t;
  }

  public Partition getP() {
    return p;
  }

  public void setP(Partition p) {
    this.p = p;
  }

  public Path getD() {
    return d;
  }

  public void setD(Path d) {
    this.d = d;
  }

  public String getClassName() {
    return this.className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getFunctionName() {
    if (typ == Type.FUNCTION) {
      return stringObject;
    }
    return null;
  }

  public void setFunctionName(String funcName) {
    if (typ != Type.FUNCTION) {
      throw new IllegalArgumentException(
          "Set function can't be called on entity if the entity type is not " + Type.FUNCTION);
    }
    this.stringObject = funcName;
  }

  public String getServiceName() {
    if (typ == Type.SERVICE_NAME) {
      return stringObject;
    }
    return null;
  }

  /**
   * Only used by serialization.
   */
  public Entity() {
    name = null;
  }

  /**
   * Constructor for a database.
   *
   * @param database
   *          Database that is read or written to.
   * @param complete
   *          Means the database is target, not for table or partition, etc.
   */
  public Entity(Database database, boolean complete) {
    this.database = database;
    this.typ = Type.DATABASE;
    this.name = computeName();
    this.complete = complete;
  }

  /**
   * Constructor for a entity with string object representation (eg SERVICE_NAME)
   *
   * @param name
   *          Used for service that action is being authorized on.
   *          Currently hostname is used for service name.
   * @param t
   *         Type of entity
   */
  public Entity(String name, Type t) {
    this.stringObject = name;
    this.typ = t;
    this.name = computeName();
  }

  /**
   * Constructor for a table.
   *
   * @param t
   *          Table that is read or written to.
   */
  public Entity(Table t, boolean complete) {
    d = null;
    p = null;
    this.t = t;
    typ = Type.TABLE;
    name = computeName();
    this.complete = complete;
  }

  /**
   * Constructor for a partition.
   *
   * @param p
   *          Partition that is read or written to.
   */
  public Entity(Partition p, boolean complete) {
    d = null;
    this.p = p;
    t = p.getTable();
    typ = Type.PARTITION;
    name = computeName();
    this.complete = complete;
  }

  public Entity(DummyPartition p, boolean complete) {
    d = null;
    this.p = p;
    t = p.getTable();
    typ = Type.DUMMYPARTITION;
    name = computeName();
    this.complete = complete;
  }

  public Entity(Path d, boolean islocal, boolean complete) {
    this.d = d;
    p = null;
    t = null;
    if (islocal) {
      typ = Type.LOCAL_DIR;
    } else {
      typ = Type.DFS_DIR;
    }
    name = computeName();
    this.complete = complete;
  }

  /**
   * Create an entity representing a object with given name, database namespace and type
   * @param database - database namespace
   * @param strObj - object name as string
   * @param className - function class name
   * @param type - the entity type. this constructor only supports FUNCTION type currently
   */
  public Entity(Database database, String strObj, String className, Type type) {
    if (type != Type.FUNCTION) {
      throw new IllegalArgumentException("This constructor is supported only for type:"
          + Type.FUNCTION);
    }
    this.database = database;
    this.stringObject = strObj;
    this.className = className;
    this.typ = type;
    this.complete = true;
    name = computeName();
  }

  /**
   * Get the parameter map of the Entity.
   */
  public Map<String, String> getParameters() {
    if (p != null) {
      return p.getParameters();
    } else {
      return t.getParameters();
    }
  }

  /**
   * Get the type of the entity.
   */
  public Type getType() {
    return typ;
  }

  public boolean isPathType() {
    return typ == Type.DFS_DIR || typ == Type.LOCAL_DIR;
  }

  /**
   * Get the location of the entity.
   */
  public URI getLocation() throws Exception {
    if (typ == Type.DATABASE) {
      String location = database.getLocationUri();
      return location == null ? null : new URI(location);
    }

    if (typ == Type.TABLE) {
      Path path = t.getDataLocation();
      return path == null ? null : path.toUri();
    }

    if (typ == Type.PARTITION) {
      Path path = p.getDataLocation();
      return path == null ? null : path.toUri();
    }

    if (typ == Type.DFS_DIR || typ == Type.LOCAL_DIR) {
      return d.toUri();
    }

    return null;
  }

  /**
   * Get the partition associated with the entity.
   */
  public Partition getPartition() {
    return p;
  }

  /**
   * Get the table associated with the entity.
   */
  public Table getTable() {
    return t;
  }

  public boolean isDummy() {
    if (typ == Type.DATABASE) {
      return database.getName().equals(SemanticAnalyzer.DUMMY_DATABASE);
    }
    if (typ == Type.TABLE) {
      return t.isDummyTable();
    }
    return false;
  }

  /**
   * toString function.
   */
  @Override
  public String toString() {
    return getName();
  }

  private String computeName() {
    return doComputeName().intern();
  }

  private String doComputeName() {
    switch (typ) {
    case DATABASE:
      return "database:" + database.getName();
    case TABLE:
      return t.getDbName() + "@" + t.getTableName();
    case PARTITION:
      return t.getDbName() + "@" + t.getTableName() + "@" + p.getName();
    case DUMMYPARTITION:
      return p.getName();
    case FUNCTION:
      if (database != null) {
        return database.getName() + "." + stringObject;
      }
      return stringObject;
    case SERVICE_NAME:
      return stringObject;
    default:
      return d.toString();
    }
  }

  /**
   * Equals function.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (o instanceof Entity) {
      Entity ore = (Entity) o;
      return (getName().equalsIgnoreCase(ore.getName()));
    } else {
      return false;
    }
  }

  /**
   * Hashcode function.
   */
  @Override
  public int hashCode() {
    return getName().hashCode();
  }

}
