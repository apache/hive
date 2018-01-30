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

/**
 * An implementation to store and manage list of ValidWriteIds for each tables read by current
 * transaction
 */
public class ValidTxnWriteIdList {
  /**
   * Key used to store valid write id list for all the operated tables in a
   * {@link org.apache.hadoop.conf.Configuration} object.
   */
  public static final String VALID_TABLES_WRITEIDS_KEY = "hive.txn.tables.valid.writeids";

  private HashMap<String, ValidWriteIdList> validTablesWriteIdList = new HashMap<>();
  public ValidTxnWriteIdList() {
  }

  public ValidTxnWriteIdList(String value) {
    readFromString(value);
  }

  @Override
  public String toString() {
    return writeToString();
  }

  public void addTableWriteId(ValidWriteIdList validWriteIds) {
    validTablesWriteIdList.put(validWriteIds.getTableName(), validWriteIds);
  }

  public ValidWriteIdList getTableWriteIdList(String tableName) {
    if (validTablesWriteIdList.containsKey(tableName)) {
      return validTablesWriteIdList.get(tableName);
    } else {
      return new ValidReaderWriteIdList();
    }
  }

  public int getNumOfTables() {
    return validTablesWriteIdList.size();
  }

  private void readFromString(String src) {
    if ((src == null) || (src.length() == 0)) {
      return;
    }
    String[] tblWriteIdStrList = src.split("\\$");
    for (String tableStr : tblWriteIdStrList) {
      ValidWriteIdList validWriteIdList = new ValidReaderWriteIdList(tableStr);
      addTableWriteId(validWriteIdList);
    }
  }

  private String writeToString() {
    StringBuilder buf = new StringBuilder();
    int index = 0;
    for (HashMap.Entry<String, ValidWriteIdList> entry : validTablesWriteIdList.entrySet()) {
      buf.append(entry.getValue().writeToString());

      // Separator for multiple tables' ValidWriteIdList. Also, skip it for last entry.
      index++;
      if (index < validTablesWriteIdList.size()) {
        buf.append('$');
      }
    }
    return buf.toString();
  }
}
