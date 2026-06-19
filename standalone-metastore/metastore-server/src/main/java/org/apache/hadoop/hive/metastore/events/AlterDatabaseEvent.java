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

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * AlterDatabaseEvent.
 * Event which is captured during database alters for owner info or properties or location
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AlterDatabaseEvent extends ListenerEvent {

  private final Database oldDb;
  private final Database newDb;
  private final boolean isReplicated;
  private final List<String> replDbProps;

  public AlterDatabaseEvent(Database oldDb, Database newDb, boolean status, IHMSHandler handler,
                            boolean isReplicated) {
    super(status, handler);
    replDbProps = MetaStoreUtils.getReplicationDbProps();
    this.oldDb = new Database(oldDb);
    this.newDb = new Database(newDb);
    // Replication Specific properties should not be captured in this event.
    // So, These props should be filtered out from both the databases;
    filterOutReplProps(this.oldDb.getParameters());
    filterOutReplProps(this.newDb.getParameters());
    this.isReplicated = isReplicated;
  }

  private void filterOutReplProps(Map<String, String> dbProps) {
    if (dbProps == null) {
      return;
    }
    List<String> propsToRemove = new ArrayList<>();
    for (Map.Entry<String, String> prop : dbProps.entrySet()) {
      String propName = prop.getKey().replace("\"", "");
      if (propName.startsWith(ReplConst.BOOTSTRAP_DUMP_STATE_KEY_PREFIX) || replDbProps.contains(propName)) {
        propsToRemove.add(prop.getKey());
      }
    }
    for (String key:propsToRemove) {
      dbProps.remove(key);
    }
  }

  /**
   * @return whether this AlterDatabaseEvent should be logged or not.
   *  (Those AlterDatabaseEvent should not be logged which alters only the replication properties of database.)
   * */
  public boolean shouldSkipCapturing() {
    if ((oldDb.getOwnerType() == newDb.getOwnerType())
            && oldDb.getOwnerName().equalsIgnoreCase(newDb.getOwnerName())) {
      // If owner information is unchanged, then DB properties would've changed
      Map<String, String> newDbProps = newDb.getParameters();
      Map<String, String> oldDbProps = oldDb.getParameters();
      return (newDbProps == null || newDbProps.isEmpty()) ? (oldDbProps == null || oldDbProps.isEmpty())
              : newDbProps.equals(oldDbProps);
    }
    return false;
  }

  /**
   * @return the old db
   */
  public Database getOldDatabase() {
    return oldDb;
  }

  /**
   * @return the new db
   */
  public Database getNewDatabase() {
    return newDb;
  }

  /**
   * @return where this event is caused by replication
   */
  public boolean isReplicated() { return isReplicated; }
}
