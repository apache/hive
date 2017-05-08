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

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InsertEvent extends ListenerEvent {

  // Note that this event is fired from the client, so rather than having full metastore objects
  // we have just the string names, but that's fine for what we need.
  private final String db;
  private final String table;
  private final Map<String, String> keyValues;
  private final List<String> files;
  private List<String> fileChecksums = new ArrayList<String>();

  /**
   *
   * @param db name of the database the table is in
   * @param table name of the table being inserted into
   * @param partVals list of partition values, can be null
   * @param insertData the inserted files & their checksums
   * @param status status of insert, true = success, false = failure
   * @param handler handler that is firing the event
   */
  public InsertEvent(String db, String table, List<String> partVals,
      InsertEventRequestData insertData, boolean status, HMSHandler handler) throws MetaException,
      NoSuchObjectException {
    super(status, handler);
    this.db = db;
    this.table = table;
    this.files = insertData.getFilesAdded();
    GetTableRequest req = new GetTableRequest(db, table);
    req.setCapabilities(HiveMetaStoreClient.TEST_VERSION);
    Table t = handler.get_table_req(req).getTable();
    keyValues = new LinkedHashMap<String, String>();
    if (partVals != null) {
      for (int i = 0; i < partVals.size(); i++) {
        keyValues.put(t.getPartitionKeys().get(i).getName(), partVals.get(i));
      }
    }
    if (insertData.isSetFilesAddedChecksum()) {
      fileChecksums = insertData.getFilesAddedChecksum();
    }
  }

  public String getDb() {
    return db;
  }

  /**
   * @return The table.
   */
  public String getTable() {
    return table;
  }

  /**
   * @return List of values for the partition keys.
   */
  public Map<String, String> getPartitionKeyValues() {
    return keyValues;
  }

  /**
   * Get list of files created as a result of this DML operation
   *
   * @return list of new files
   */
  public List<String> getFiles() {
    return files;
  }

  /**
   * Get a list of file checksums corresponding to the files created (if available)
   *
   * @return
   */
  public List<String> getFileChecksums() {
    return fileChecksums;
  }
}
