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

package org.apache.hadoop.hive.common;

import java.util.HashMap;
import java.util.Map;

/**
 * An implementation to store and manage list of ValidWriteIds for each tables read by current
 * transaction.
 */
public class ValidTxnWriteIdList {
  /**
   * Key used to store valid write id list for all the operated tables in a
   * {@link org.apache.hadoop.conf.Configuration} object.
   */
  public static final String VALID_TABLES_WRITEIDS_KEY = "hive.txn.tables.valid.writeids";

  // Transaction for which the list of tables valid write Ids are populated
  private Long txnId;

  // Map of valid write ids list for all the tables read by the current txn
  // Key is full table name string of format <db_name>.<table_name>
  private Map<String, ValidWriteIdList> tablesValidWriteIdList = new HashMap<>();
  public ValidTxnWriteIdList(Long txnId) {
    this.txnId = txnId;
  }

  public ValidTxnWriteIdList(String value) {
    readFromString(value);
  }

  @Override
  public String toString() {
    return writeToString();
  }

  public void addTableValidWriteIdList(ValidWriteIdList validWriteIds) {
    tablesValidWriteIdList.put(validWriteIds.getTableName(), validWriteIds);
  }

  // Input fullTableName is of format <db_name>.<table_name>
  public ValidWriteIdList getTableValidWriteIdList(String fullTableName) {
    if (tablesValidWriteIdList.containsKey(fullTableName)) {
      return tablesValidWriteIdList.get(fullTableName);
    }
    return null;
  }

  public boolean isEmpty() {
    return tablesValidWriteIdList.isEmpty();
  }

  // Each ValidWriteIdList is separated with "$" and each one maps to one table
  // Format <txnId>$<table_name>:<hwm>:<minOpenWriteId>:<open_writeids>:<abort_writeids>$<table_name>...
  private void readFromString(String src) {
    if ((src == null) || (src.length() == 0)) {
      return;
    }
    String[] tblWriteIdStrList = src.split("\\$");
    assert(tblWriteIdStrList.length >= 1);

    // First $ separated substring would be txnId and the rest are ValidReaderWriteIdList
    this.txnId = Long.parseLong(tblWriteIdStrList[0]);
    for (int index = 1; index < tblWriteIdStrList.length; index++) {
      String tableStr = tblWriteIdStrList[index];
      ValidWriteIdList validWriteIdList = new ValidReaderWriteIdList(tableStr);
      addTableValidWriteIdList(validWriteIdList);
    }
  }

  // Each ValidWriteIdList is separated with "$" and each one maps to one table
  // Format <txnId>$<table_name>:<hwm>:<minOpenWriteId>:<open_writeids>:<abort_writeids>$<table_name>...
  private String writeToString() {
    // First $ separated substring will be txnId and the rest are ValidReaderWriteIdList
    StringBuilder buf = new StringBuilder(txnId.toString());
    int index = 0;
    for (HashMap.Entry<String, ValidWriteIdList> entry : tablesValidWriteIdList.entrySet()) {
      if (index < tablesValidWriteIdList.size()) {
        buf.append('$');
      }
      buf.append(entry.getValue().writeToString());

      // Separator for multiple tables' ValidWriteIdList. Also, skip it for last entry.
      index++;
    }
    return buf.toString();
  }
}
