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
package org.apache.hadoop.hive.ql.parse.repl.load;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

/**
 * Utility class to help return complex value from readMetaData function
 */
public class MetaData {
  private final Database db;
  private final Table table;
  private final Iterable<Partition> partitions;
  private final ReplicationSpec replicationSpec;
  public final Function function;

  public MetaData() {
    this(null, null, null, new ReplicationSpec(), null);
  }

  MetaData(Database db, Table table, Iterable<Partition> partitions,
      ReplicationSpec replicationSpec, Function function) {
    this.db = db;
    this.table = table;
    this.partitions = partitions;
    this.replicationSpec = replicationSpec;
    this.function = function;
  }

  public Database getDatabase() {
    return db;
  }

  public Table getTable() {
    return table;
  }

  public Iterable<Partition> getPartitions() {
    return partitions;
  }

  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }
}
