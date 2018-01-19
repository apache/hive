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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * This class encapsulates the information on the partition and tables that are
 * read by the query.
 */
public class ReadEntity extends Entity implements Serializable {

  // Consider a query like: select * from V, where the view V is defined as:
  // select * from T
  // The inputs will contain V and T (parent: V)
  // T will be marked as an indirect entity using isDirect flag.
  // This will help in distinguishing from the case where T is a direct dependency
  // For example in the case of "select * from V join T ..." T would be direct dependency
  private boolean isDirect = true;

  // Note that we do not need a lock for this entity.  This is used by operations like alter
  // table ... partition where its actually the partition that needs locked even though the table
  // is marked as being read.  Defaults to true as that is the most common case.
  private boolean needsLock = true;

  // When true indicates that this object is being read as part of an update or delete.  This is
  // important because in that case we shouldn't acquire a lock for it or authorize the read.
  // These will be handled by the output to the table instead.
  private boolean isUpdateOrDelete = false;
  /**
   * https://issues.apache.org/jira/browse/HIVE-15048
   * It is possible that the same table is used in top level query and a sub-query, e.g.
   * select * from T where T.c in (select c from T inner join S on T.a=S.b)
   */
  public transient boolean isFromTopLevelQuery = true;


  // For views, the entities can be nested - by default, entities are at the top level
  // Must be deterministic order set for consistent q-test output across Java versions
  private final Set<ReadEntity> parents = new LinkedHashSet<ReadEntity>();

  // The accessed columns of query
  private final List<String> accessedColumns = new ArrayList<String>();

  /**
   * For serialization only.
   */
  public ReadEntity() {
    super();
  }

  /**
   * Constructor for a database.
   */
  public ReadEntity(Database database) {
    super(database, true);
  }

  /**
   * Constructor.
   *
   * @param t
   *          The Table that the query reads from.
   */
  public ReadEntity(Table t) {
    super(t, true);
  }

  private void initParent(ReadEntity parent) {
    if (parent != null) {
      this.parents.add(parent);
    }
  }

  public ReadEntity(Table t, ReadEntity parent) {
    super(t, true);
    initParent(parent);
  }

  public ReadEntity(Table t, ReadEntity parent, boolean isDirect) {
    this(t, parent);
    this.isDirect = isDirect;
  }

  /**
   * Constructor given a partition.
   *
   * @param p
   *          The partition that the query reads from.
   */
  public ReadEntity(Partition p) {
    super(p, true);
  }

  public ReadEntity(Partition p, ReadEntity parent) {
    super(p, true);
    initParent(parent);
  }

  public ReadEntity(Partition p, ReadEntity parent, boolean isDirect) {
    this(p, parent);
    this.isDirect = isDirect;
  }

  /**
   * Constructor for a file.
   *
   * @param d
   *          The name of the directory that is being written to.
   * @param islocal
   *          Flag to decide whether this directory is local or in dfs.
   */
  public ReadEntity(Path d, boolean islocal) {
    super(d, islocal, true);
  }

  public Set<ReadEntity> getParents() {
    return parents;
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
      return (getName().equalsIgnoreCase(ore.getName()));
    } else {
      return false;
    }
  }

  public boolean isDirect() {
    return isDirect;
  }

  public void setDirect(boolean isDirect) {
    this.isDirect = isDirect;
  }

  public boolean needsLock() {
    return needsLock;
  }

  public void noLockNeeded() {
    needsLock = false;
  }

  public List<String> getAccessedColumns() {
    return accessedColumns;
  }

  public void setUpdateOrDelete(boolean isUpdateOrDelete) {
    this.isUpdateOrDelete = isUpdateOrDelete;
  }

  public boolean isUpdateOrDelete() {
    return isUpdateOrDelete;
  }
}
