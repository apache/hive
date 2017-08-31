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
package org.apache.hadoop.hive.ql.parse.repl.dump;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

/**
 * The idea for this class is that since we need to make sure that
 * we query the replication id from the db before we do any queries
 * to get the object from metastore like tables/functions/partitions etc
 * we are devising this wrapper to wrap all such ordering of statements here.
 */

public class HiveWrapper {
  private final Hive db;
  private final String dbName;
  private final Tuple.Function<ReplicationSpec> functionForSpec;

  public HiveWrapper(Hive db, String dbName) {
    this.dbName = dbName;
    this.db = db;
    this.functionForSpec = new BootStrapReplicationSpecFunction(db);
  }

  public Tuple<org.apache.hadoop.hive.metastore.api.Function> function(final String name)
      throws HiveException {
    return new Tuple<>(functionForSpec, () -> db.getFunction(dbName, name));
  }

  public Tuple<Database> database() throws HiveException {
    return new Tuple<>(functionForSpec, () -> db.getDatabase(dbName));
  }

  public Tuple<Table> table(final String tableName) throws HiveException {
    return new Tuple<>(functionForSpec, () -> db.getTable(dbName, tableName));
  }

  public static class Tuple<T> {

    interface Function<T> {
      T fromMetaStore() throws HiveException;
    }

    public final ReplicationSpec replicationSpec;
    public final T object;

    /**
     * we have to get the replicationspec before we query for the function object
     * from the hive metastore as the spec creation captures the latest event id for replication
     * and we dont want to miss any events hence we are ok replaying some events as part of
     * incremental load to achieve a consistent state of the warehouse.
     */
    Tuple(Function<ReplicationSpec> replicationSpecFunction,
        Function<T> functionForObject) throws HiveException {
      this.replicationSpec = replicationSpecFunction.fromMetaStore();
      this.object = functionForObject.fromMetaStore();
    }
  }
}
