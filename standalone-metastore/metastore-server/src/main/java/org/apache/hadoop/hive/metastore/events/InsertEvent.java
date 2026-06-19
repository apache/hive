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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.ClientCapabilities;
import org.apache.hadoop.hive.metastore.api.ClientCapability;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class InsertEvent extends ListenerEvent {

  private final Table tableObj;
  private final Partition ptnObj;
  private final boolean replace;
  private final List<String> files;
  private List<String> fileChecksums = new ArrayList<>();

  /**
   *
   * @param db name of the database the table is in
   * @param table name of the table being inserted into
   * @param partVals list of partition values, can be null
   * @param insertData the inserted files and their checksums
   * @param status status of insert, true = success, false = failure
   * @param handler handler that is firing the event
   */
  public InsertEvent(String catName, String db, String table, List<String> partVals,
      InsertEventRequestData insertData, boolean status, IHMSHandler handler) throws MetaException,
      NoSuchObjectException {
    super(status, handler);

    GetTableRequest req = new GetTableRequest(db, table);
    req.setCatName(catName);
    // TODO MS-SPLIT Switch this back once HiveMetaStoreClient is moved.
    //req.setCapabilities(HiveMetaStoreClient.TEST_VERSION);
    req.setCapabilities(new ClientCapabilities(
        Lists.newArrayList(ClientCapability.TEST_CAPABILITY, ClientCapability.INSERT_ONLY_TABLES)));
    try {
      this.tableObj = handler.get_table_req(req).getTable();
      if (partVals != null) {
        this.ptnObj = handler.get_partition(MetaStoreUtils.prependNotNullCatToDbName(catName, db),
            table, partVals);
      } else {
        this.ptnObj = null;
      }
    } catch (NoSuchObjectException e) {
      // This is to mimic previous behavior where NoSuchObjectException was thrown through this
      // method.
      throw e;
    } catch (TException e) {
      throw MetaStoreUtils.newMetaException(e);
    }

    // If replace flag is not set by caller, then by default set it to true to maintain backward compatibility
    this.replace = (insertData.isSetReplace() ? insertData.isReplace() : true);
    this.files = insertData.getFilesAdded();
    if (insertData.isSetFilesAddedChecksum()) {
      fileChecksums = insertData.getFilesAddedChecksum();
    }
  }

  /**
   * @return Table object
   */
  public Table getTableObj() {
    return tableObj;
  }

  /**
   * @return Partition object
   */
  public Partition getPartitionObj() {
    return ptnObj;
  }

  /**
   * @return The replace flag.
   */
  public boolean isReplace() {
    return replace;
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
