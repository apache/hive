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
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * The idea for this class is that since we need to make sure that
 * we query the replication id from the db before we do any queries
 * to get the object from metastore like tables/functions/partitions etc
 * we are devising this wrapper to wrap all such ordering of statements here.
 */

public class HiveWrapper {
  private final Hive db;
  private final String dbName;
  private final ReplicationSpecFromMetaStore functionForSpec;

  public HiveWrapper(Hive db, String dbName) {
    this.dbName = dbName;
    this.db = db;
    this.functionForSpec = new ReplicationSpecFromMetaStore(db);
  }

  public Tuple<Function> function(final String name) throws HiveException {
    return new Tuple<>(functionForSpec, () -> db.getFunction(dbName, name));
  }

  public Tuple<Database> database() throws HiveException {
    return new Tuple<>(functionForSpec, () -> db.getDatabase(dbName));
  }

  private static class ReplicationSpecFromMetaStore
      implements Tuple.GetFromMetaStore<ReplicationSpec> {
    private final Hive db;

    ReplicationSpecFromMetaStore(Hive db) {
      this.db = db;
    }

    @Override
    public ReplicationSpec object() throws HiveException {
      try {
        ReplicationSpec replicationSpec =
            new ReplicationSpec(
                true,
                false,
                "replv2",
                "will-be-set",
                false,
                true,
                false
            );
        long currentNotificationId = db.getMSC()
            .getCurrentNotificationEventId().getEventId();
        replicationSpec.setCurrentReplicationState(String.valueOf(currentNotificationId));
        return replicationSpec;
      } catch (Exception e) {
        throw new SemanticException(e);
        // TODO : simple wrap & rethrow for now, clean up with error codes
      }
    }
  }

  public static class Tuple<T> {

    interface GetFromMetaStore<T> {
      T object() throws HiveException;
    }

    public final ReplicationSpec replicationSpec;
    public final T object;

    /**
     * we have to get the replicationspec before we query for the function object
     * from the hive metastore as the spec creation captures the latest event id for replication
     * and we dont want to miss any events hence we are ok replaying some events as part of
     * incremental load to achieve a consistent state of the warehouse.
     */
    Tuple(GetFromMetaStore<ReplicationSpec> replicationSpecFunction,
        GetFromMetaStore<T> functionForObject) throws HiveException {
      this.replicationSpec = replicationSpecFunction.object();
      this.object = functionForObject.object();
    }
  }
}
