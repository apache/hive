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

package org.apache.hadoop.hive.ql.parse;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Implementation of the metadata information related to a query block.
 * 
 **/

public class QBMetaData {

  public static final int DEST_INVALID = 0;
  public static final int DEST_TABLE = 1;
  public static final int DEST_PARTITION = 2;
  public static final int DEST_DFS_FILE = 3;
  public static final int DEST_REDUCE = 4;
  public static final int DEST_LOCAL_FILE = 5;

  private final HashMap<String, Table> aliasToTable;
  private final HashMap<String, Table> nameToDestTable;
  private final HashMap<String, Partition> nameToDestPartition;
  private final HashMap<String, String> nameToDestFile;
  private final HashMap<String, Integer> nameToDestType;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(QBMetaData.class.getName());

  public QBMetaData() {
    aliasToTable = new HashMap<String, Table>();
    nameToDestTable = new HashMap<String, Table>();
    nameToDestPartition = new HashMap<String, Partition>();
    nameToDestFile = new HashMap<String, String>();
    nameToDestType = new HashMap<String, Integer>();
  }

  // All getXXX needs toLowerCase() because they are directly called from
  // SemanticAnalyzer
  // All setXXX does not need it because they are called from QB which already
  // lowercases
  // the aliases.

  public HashMap<String, Table> getAliasToTable() {
    return aliasToTable;
  }

  public Table getTableForAlias(String alias) {
    return aliasToTable.get(alias.toLowerCase());
  }

  public void setSrcForAlias(String alias, Table tab) {
    aliasToTable.put(alias, tab);
  }

  public void setDestForAlias(String alias, Table tab) {
    nameToDestType.put(alias, Integer.valueOf(DEST_TABLE));
    nameToDestTable.put(alias, tab);
  }

  public void setDestForAlias(String alias, Partition part) {
    nameToDestType.put(alias, Integer.valueOf(DEST_PARTITION));
    nameToDestPartition.put(alias, part);
  }

  public void setDestForAlias(String alias, String fname, boolean isDfsFile) {
    nameToDestType.put(alias, isDfsFile ? Integer.valueOf(DEST_DFS_FILE)
        : Integer.valueOf(DEST_LOCAL_FILE));
    nameToDestFile.put(alias, fname);
  }

  public Integer getDestTypeForAlias(String alias) {
    return nameToDestType.get(alias.toLowerCase());
  }

  public Table getDestTableForAlias(String alias) {
    return nameToDestTable.get(alias.toLowerCase());
  }

  public Partition getDestPartitionForAlias(String alias) {
    return nameToDestPartition.get(alias.toLowerCase());
  }

  public String getDestFileForAlias(String alias) {
    return nameToDestFile.get(alias.toLowerCase());
  }

  public Table getSrcForAlias(String alias) {
    return aliasToTable.get(alias.toLowerCase());
  }
}
