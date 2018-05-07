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
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;

/**
 * AlterDatabaseEvent.
 * Event which is captured during database alters for owner info or properties or location
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AlterDatabaseEvent extends ListenerEvent {

  private final Database oldDb;
  private final Database newDb;

  public AlterDatabaseEvent(Database oldDb, Database newDb, boolean status, IHMSHandler handler) {
    super(status, handler);
    this.oldDb = oldDb;
    this.newDb = newDb;
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
}
