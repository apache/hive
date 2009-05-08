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

import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;
import java.net.URI;

/**
 * This class encapsulates an object that is being written to
 * by the query. This object may be a table, partition, dfs
 * directory or a local directory.
 */
public class WriteEntity {

  /**
   * The type of the write entity.
   */
  public static enum Type {TABLE, PARTITION, DFS_DIR, LOCAL_DIR};
  
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
   * The directory if this is a directory.
   */
  private String d;
  
  /**
   * Constructor for a table.
   * 
   * @param t Table that is written to.
   */
  public WriteEntity(Table t) {
    this.d = null;
    this.p = null;
    this.t = t;
    this.typ = Type.TABLE;
  }
  
  /**
   * Constructor for a partition.
   * 
   * @param p Partition that is written to.
   */
  public WriteEntity(Partition p) {
    this.d = null;
    this.p = p;
    this.t = p.getTable();
    this.typ = Type.PARTITION;
  }
  
  /**
   * Constructor for a file.
   * 
   * @param d The name of the directory that is being written to.
   * @param islocal Flag to decide whether this directory is local or in dfs.
   */
  public WriteEntity(String d, boolean islocal) {
    this.d = d;
    this.p = null;
    this.t = null;
    if (islocal) {
      this.typ = Type.LOCAL_DIR;
    }
    else {
      this.typ = Type.DFS_DIR;
    }
  }
  
  /**
   * Get the type of the entity.
   */
  public Type getType() {
    return typ;
  }
  
  /**
   * Get the location of the entity.
   */
  public URI getLocation() throws Exception {
    if (typ == Type.TABLE)
      return t.getDataLocation();
    
    if (typ == Type.PARTITION)
      return p.getDataLocation();
    
    if (typ == Type.DFS_DIR || typ == Type.LOCAL_DIR)
      return new URI(d);
    
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

  /**
   * toString function.
   */
  public String toString() {
    switch(typ) {
    case TABLE:
      return t.getDbName() + "/" + t.getName();
    case PARTITION:
      return t.getDbName() + "/" + t.getName() + "/" + p.getName();
    default:
      return d;
    }
  }
  
  /**
   * Equals function.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null)
      return false;
    
    if (o instanceof WriteEntity) {
      WriteEntity ore = (WriteEntity)o;
      return (toString().equalsIgnoreCase(ore.toString()));
    }
    else
      return false;
  }
  
  /**
   * Hashcode function.
   */
  @Override
  public int hashCode() {
    return toString().hashCode();
  }

}
