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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.io.Serializable;

/**
 * This class encapsulates an object that is being written to by the query. This
 * object may be a table, partition, dfs directory or a local directory.
 */
public class WriteEntity extends Entity implements Serializable {

  private boolean isTempURI = false;

  public static enum WriteType {
    DDL, // for use in DDL statements that will touch data,
         // will result in an exclusive lock,
    DDL_METADATA_ONLY, // for use in DDL statements that touch only
                       // metadata and don't need a lock
    INSERT,
    INSERT_OVERWRITE,
    UPDATE,
    DELETE};

  private WriteType writeType;

  /**
   * Only used by serialization.
   */
  public WriteEntity() {
    super();
  }

  public WriteEntity(Database database, WriteType type) {
    super(database, true);
    writeType = type;
  }

  /**
   * Constructor for a table.
   *
   * @param t
   *          Table that is written to.
   */
  public WriteEntity(Table t, WriteType type) {
    super(t, true);
    writeType = type;
  }

  public WriteEntity(Table t, WriteType type, boolean complete) {
    super(t, complete);
    writeType = type;
  }

  /**
   * Constructor for a partition.
   *
   * @param p
   *          Partition that is written to.
   */
  public WriteEntity(Partition p, WriteType type) {
    super(p, true);
    writeType = type;
  }

  public WriteEntity(DummyPartition p, WriteType type, boolean complete) {
    super(p, complete);
    writeType = type;
  }

  /**
   * Constructor for a file.
   *
   * @param d
   *          The name of the directory that is being written to.
   * @param islocal
   *          Flag to decide whether this directory is local or in dfs.
   */
  public WriteEntity(Path d, boolean islocal) {
    this(d, islocal, false);
  }

  /**
   * Constructor for a file.
   *
   * @param d
   *          The name of the directory that is being written to.
   * @param islocal
   *          Flag to decide whether this directory is local or in dfs.
   * @param isTemp
   *          True if this is a temporary location such as scratch dir
   */
  public WriteEntity(Path d, boolean islocal, boolean isTemp) {
    super(d.toString(), islocal, true);
    this.isTempURI = isTemp;
  }

  /**
   * Determine which type of write this is.  This is needed by the lock
   * manager so it can understand what kind of lock to acquire.
   * @return write type
   */
  public WriteType getWriteType() {
    return writeType;
  }

  /**
   * Equals function.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (o instanceof WriteEntity) {
      WriteEntity ore = (WriteEntity) o;
      return (toString().equalsIgnoreCase(ore.toString()));
    } else {
      return false;
    }
  }

  public boolean isTempURI() {
    return isTempURI;
  }

}
