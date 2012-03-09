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

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * This class encapsulates an object that is being written to by the query. This
 * object may be a table, partition, dfs directory or a local directory.
 */
public class WriteEntity extends Entity implements Serializable {

  /**
   * Only used by serialization.
   */
  public WriteEntity() {
    super();
  }

  /**
   * Constructor for a table.
   *
   * @param t
   *          Table that is written to.
   */
  public WriteEntity(Table t) {
    this(t, true);
  }

  public WriteEntity(Table t, boolean complete) {
    super(t, complete);
  }

  /**
   * Constructor for a partition.
   *
   * @param p
   *          Partition that is written to.
   */
  public WriteEntity(Partition p) {
    this(p, true);
  }

  public WriteEntity(Partition p, boolean complete) {
    super(p, complete);
  }

  public WriteEntity(DummyPartition p, boolean complete) {
    super(p, complete);
  }

  /**
   * Constructor for a file.
   *
   * @param d
   *          The name of the directory that is being written to.
   * @param islocal
   *          Flag to decide whether this directory is local or in dfs.
   */
  public WriteEntity(String d, boolean islocal) {
    this(d, islocal, true);
  }

  public WriteEntity(String d, boolean islocal, boolean complete) {
    super(d, islocal, complete);
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
}
