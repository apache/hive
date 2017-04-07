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

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

class BootStrapReplicationSpecFunction implements HiveWrapper.Tuple.Function<ReplicationSpec> {
  private final Hive db;

  BootStrapReplicationSpecFunction(Hive db) {
    this.db = db;
  }

  @Override
  public ReplicationSpec fromMetaStore() throws HiveException {
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
