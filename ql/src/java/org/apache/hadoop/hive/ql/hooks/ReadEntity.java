/**
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

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * This class encapsulates the information on the partition and tables that are
 * read by the query.
 */
public class ReadEntity implements Serializable {

  private static final long serialVersionUID = 1L;
  
  /**
   * The table.
   */
  private Table t;

  /**
   * The partition. This is null for a non partitioned table.
   */
  private Partition p;

  /**
   * This is derived from t and p, but we need to serialize this field to make sure
   * ReadEntity.hashCode() does not need to recursively read into t and p. 
   */
  private String name;
  
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setP(Partition p) {
    this.p = p;
  }

  public void setT(Table t) {
    this.t = t;
  }

  public Partition getP() {
    return p;
  }

  public Table getT() {
    return t;
  }

  /**
   * For serialization only.
   */
  public ReadEntity() {
  }
  
  /**
   * Constructor.
   * 
   * @param t
   *          The Table that the query reads from.
   */
  public ReadEntity(Table t) {
    this.t = t;
    p = null;
    name = computeName();
  }

  /**
   * Constructor given a partiton.
   * 
   * @param p
   *          The partition that the query reads from.
   */
  public ReadEntity(Partition p) {
    t = p.getTable();
    this.p = p;
    name = computeName();
  }

  private String computeName() {
    if (p != null) {
      return p.getTable().getDbName() + "@" + p.getTable().getTableName() + "@"
          + p.getName();
    } else {
      return t.getDbName() + "@" + t.getTableName();
    }
  }
  
  /**
   * Enum that tells what time of a read entity this is.
   */
  public static enum Type {
    TABLE, PARTITION
  };

  /**
   * Get the type.
   */
  public Type getType() {
    return p == null ? Type.TABLE : Type.PARTITION;
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
   * Get the location of the entity.
   */
  public URI getLocation() {
    if (p != null) {
      return p.getDataLocation();
    } else {
      return t.getDataLocation();
    }
  }

  /**
   * Get partition entity.
   */
  public Partition getPartition() {
    return p;
  }

  /**
   * Get table entity.
   */
  public Table getTable() {
    return t;
  }

  /**
   * toString function.
   */
  @Override
  public String toString() {
    return name;
  }

  /**
   * Equals function.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (o instanceof ReadEntity) {
      ReadEntity ore = (ReadEntity) o;
      return (toString().equalsIgnoreCase(ore.toString()));
    } else {
      return false;
    }
  }

  /**
   * Hashcode function.
   */
  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
