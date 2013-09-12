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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.hbase.snapshot;


/**
 * The PathUtil class is a utility class to provide information about various
 * znode paths. The following is the znode structure used for storing information.
 * baseDir/ClockNode
 * baseDir/TrasactionBasePath
 * baseDir/TrasactionBasePath/TableA/revisionID
 * baseDir/TrasactionBasePath/TableA/columnFamily-1
 * baseDir/TrasactionBasePath/TableA/columnFamily-1/runnningTxns
 * baseDir/TrasactionBasePath/TableA/columnFamily-1/abortedTxns
 * baseDir/TrasactionBasePath/TableB/revisionID
 * baseDir/TrasactionBasePath/TableB/columnFamily-1
 * baseDir/TrasactionBasePath/TableB/columnFamily-1/runnningTxns
 * baseDir/TrasactionBasePath/TableB/columnFamily-1/abortedTxns

 */
public class PathUtil {

  static final String DATA_DIR = "/data";
  static final String CLOCK_NODE = "/clock";

  /**
   * This method returns the data path associated with the currently
   * running transactions of a given table and column/column family.
   * @param baseDir
   * @param tableName
   * @param columnFamily
   * @return The path of the running transactions data.
   */
  static String getRunningTxnInfoPath(String baseDir, String tableName,
                    String columnFamily) {
    String txnBasePath = getTransactionBasePath(baseDir);
    String path = txnBasePath + "/" + tableName + "/" + columnFamily
      + "/runningTxns";
    return path;
  }

  /**
   * This method returns the data path associated with the aborted
   * transactions of a given table and column/column family.
   * @param baseDir The base directory for revision management.
   * @param tableName The name of the table.
   * @param columnFamily
   * @return The path of the aborted transactions data.
   */
  static String getAbortInformationPath(String baseDir, String tableName,
                      String columnFamily) {
    String txnBasePath = getTransactionBasePath(baseDir);
    String path = txnBasePath + "/" + tableName + "/" + columnFamily
      + "/abortData";
    return path;
  }

  /**
   * Gets the revision id node for a given table.
   *
   * @param baseDir the base dir for revision management.
   * @param tableName the table name
   * @return the revision id node path.
   */
  static String getRevisionIDNode(String baseDir, String tableName) {
    String rmBasePath = getTransactionBasePath(baseDir);
    String revisionIDNode = rmBasePath + "/" + tableName + "/idgen";
    return revisionIDNode;
  }

  /**
   * Gets the lock management node for any znode that needs to be locked.
   *
   * @param path the path of the znode.
   * @return the lock management node path.
   */
  static String getLockManagementNode(String path) {
    String lockNode = path + "_locknode_";
    return lockNode;
  }

  /**
   * This method returns the base path for the transaction data.
   *
   * @param baseDir The base dir for revision management.
   * @return The base path for the transaction data.
   */
  static String getTransactionBasePath(String baseDir) {
    String txnBaseNode = baseDir + DATA_DIR;
    return txnBaseNode;
  }

  /**
   * Gets the txn data path for a given table.
   *
   * @param baseDir the base dir for revision management.
   * @param tableName the table name
   * @return the txn data path for the table.
   */
  static String getTxnDataPath(String baseDir, String tableName) {
    String txnBasePath = getTransactionBasePath(baseDir);
    String path = txnBasePath + "/" + tableName;
    return path;
  }

  /**
   * This method returns the data path for clock node.
   *
   * @param baseDir
   * @return The data path for clock.
   */
  static String getClockPath(String baseDir) {
    String clockNode = baseDir + CLOCK_NODE;
    return clockNode;
  }
}
