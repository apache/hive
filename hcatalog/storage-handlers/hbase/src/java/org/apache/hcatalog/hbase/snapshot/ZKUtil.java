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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hcatalog.hbase.snapshot.transaction.thrift.StoreFamilyRevision;
import org.apache.hcatalog.hbase.snapshot.transaction.thrift.StoreFamilyRevisionList;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZKUtil {

  private int DEFAULT_SESSION_TIMEOUT = 1000000;
  private ZooKeeper zkSession;
  private String baseDir;
  private String connectString;
  private static final Logger LOG = LoggerFactory.getLogger(ZKUtil.class);

  static enum UpdateMode {
    APPEND, REMOVE, KEEP_ALIVE
  }

  ;

  ZKUtil(String connection, String baseDir) {
    this.connectString = connection;
    this.baseDir = baseDir;
  }

  /**
   * This method creates znodes related to table.
   *
   * @param table The name of the table.
   * @param families The list of column families of the table.
   * @throws IOException
   */
  void setUpZnodesForTable(String table, List<String> families)
    throws IOException {

    String transactionDataTablePath = PathUtil.getTxnDataPath(baseDir, table);
    ensurePathExists(transactionDataTablePath, null, Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
    for (String cf : families) {
      String runningDataPath = PathUtil.getRunningTxnInfoPath(
        this.baseDir, table, cf);
      ensurePathExists(runningDataPath, null, Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
      String abortDataPath = PathUtil.getAbortInformationPath(
        this.baseDir, table, cf);
      ensurePathExists(abortDataPath, null, Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    }

  }

  /**
   * This method ensures that a given path exists in zookeeper. If the path
   * does not exists, it creates one.
   *
   * @param path The path of znode that is required to exist.
   * @param data The data to be associated with the znode.
   * @param acl The ACLs required.
   * @param flags The CreateMode for the znode.
   * @throws IOException
   */
  void ensurePathExists(String path, byte[] data, List<ACL> acl,
              CreateMode flags) throws IOException {
    String[] dirs = path.split("/");
    String parentPath = "";
    for (String subDir : dirs) {
      if (subDir.equals("") == false) {
        parentPath = parentPath + "/" + subDir;
        try {
          Stat stat = getSession().exists(parentPath, false);
          if (stat == null) {
            getSession().create(parentPath, data, acl, flags);
          }
        } catch (Exception e) {
          throw new IOException("Exception while creating path "
            + parentPath, e);
        }
      }
    }

  }

  /**
   * This method returns a list of columns of a table which were used in any
   * of the transactions.
   *
   * @param tableName The name of table.
   * @return List<String> The list of column families in table.
   * @throws IOException
   */
  List<String> getColumnFamiliesOfTable(String tableName) throws IOException {
    String path = PathUtil.getTxnDataPath(baseDir, tableName);
    List<String> children = null;
    List<String> columnFamlies = new ArrayList<String>();
    try {
      children = getSession().getChildren(path, false);
    } catch (KeeperException e) {
      LOG.warn("Caught: ", e);
      throw new IOException("Exception while obtaining columns of table.", e);
    } catch (InterruptedException e) {
      LOG.warn("Caught: ", e);
      throw new IOException("Exception while obtaining columns of table.", e);
    }

    for (String child : children) {
      if ((child.contains("idgen") == false)
        && (child.contains("_locknode_") == false)) {
        columnFamlies.add(child);
      }
    }
    return columnFamlies;
  }

  /**
   * This method returns a time stamp for use by the transactions.
   *
   * @return long The current timestamp in zookeeper.
   * @throws IOException
   */
  long getTimeStamp() throws IOException {
    long timeStamp;
    Stat stat;
    String clockPath = PathUtil.getClockPath(this.baseDir);
    ensurePathExists(clockPath, null, Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
    try {
      getSession().exists(clockPath, false);
      stat = getSession().setData(clockPath, null, -1);

    } catch (KeeperException e) {
      LOG.warn("Caught: ", e);
      throw new IOException("Exception while obtaining timestamp ", e);
    } catch (InterruptedException e) {
      LOG.warn("Caught: ", e);
      throw new IOException("Exception while obtaining timestamp ", e);
    }
    timeStamp = stat.getMtime();
    return timeStamp;
  }

  /**
   * This method returns the next revision number to be used for any
   * transaction purposes.
   *
   * @param tableName The name of the table.
   * @return revision number The revision number last used by any transaction.
   * @throws IOException
   */
  long nextId(String tableName) throws IOException {
    String idNode = PathUtil.getRevisionIDNode(this.baseDir, tableName);
    ensurePathExists(idNode, Bytes.toBytes("0"), Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
    String lockNode = PathUtil.getLockManagementNode(idNode);
    ensurePathExists(lockNode, null, Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
    IDGenerator idf = new IDGenerator(getSession(), tableName, idNode);
    long id = idf.obtainID();
    return id;
  }

  /**
   * The latest used revision id of the table.
   *
   * @param tableName The name of the table.
   * @return the long The revision number to use by any transaction.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  long currentID(String tableName) throws IOException {
    String idNode = PathUtil.getRevisionIDNode(this.baseDir, tableName);
    ensurePathExists(idNode, Bytes.toBytes("0"), Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
    String lockNode = PathUtil.getLockManagementNode(idNode);
    ensurePathExists(lockNode, null, Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
    IDGenerator idf = new IDGenerator(getSession(), tableName, idNode);
    long id = idf.readID();
    return id;
  }

  /**
   * This methods retrieves the list of transaction information associated
   * with each column/column family of a table.
   *
   * @param path The znode path
   * @return List of FamilyRevision The list of transactions in the given path.
   * @throws IOException
   */
  List<FamilyRevision> getTransactionList(String path)
    throws IOException {

    byte[] data = getRawData(path, new Stat());
    ArrayList<FamilyRevision> wtxnList = new ArrayList<FamilyRevision>();
    if (data == null) {
      return wtxnList;
    }
    StoreFamilyRevisionList txnList = new StoreFamilyRevisionList();
    deserialize(txnList, data);
    Iterator<StoreFamilyRevision> itr = txnList.getRevisionListIterator();

    while (itr.hasNext()) {
      StoreFamilyRevision wtxn = itr.next();
      wtxnList.add(new FamilyRevision(wtxn.getRevision(), wtxn
        .getTimestamp()));
    }

    return wtxnList;
  }

  /**
   * This method returns the data associated with the path in zookeeper.
   *
   * @param path The znode path
   * @param stat Zookeeper stat
   * @return byte array The data stored in the znode.
   * @throws IOException
   */
  byte[] getRawData(String path, Stat stat) throws IOException {
    byte[] data = null;
    try {
      data = getSession().getData(path, false, stat);
    } catch (Exception e) {
      throw new IOException(
        "Exception while obtaining raw data from zookeeper path "
          + path, e);
    }
    return data;
  }

  /**
   * This method created the basic znodes in zookeeper for revision
   * management.
   *
   * @throws IOException
   */
  void createRootZNodes() throws IOException {
    String txnBaseNode = PathUtil.getTransactionBasePath(this.baseDir);
    String clockNode = PathUtil.getClockPath(this.baseDir);
    ensurePathExists(txnBaseNode, null, Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
    ensurePathExists(clockNode, null, Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
  }

  /**
   * This method closes the zookeeper session.
   */
  void closeZKConnection() {
    if (zkSession != null) {
      try {
        zkSession.close();
      } catch (InterruptedException e) {
        LOG.warn("Close failed: ", e);
      }
      zkSession = null;
      LOG.info("Disconnected to ZooKeeper");
    }
  }

  /**
   * This method returns a zookeeper session. If the current session is closed,
   * then a new session is created.
   *
   * @return ZooKeeper An instance of zookeeper client.
   * @throws IOException
   */
  ZooKeeper getSession() throws IOException {
    if (zkSession == null || zkSession.getState() == States.CLOSED) {
      synchronized (this) {
        if (zkSession == null || zkSession.getState() == States.CLOSED) {
          zkSession = new ZooKeeper(this.connectString,
            this.DEFAULT_SESSION_TIMEOUT, new ZKWatcher());
          while (zkSession.getState() == States.CONNECTING) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
          }
        }
      }
    }
    return zkSession;
  }

  /**
   * This method updates the transaction data related to a znode.
   *
   * @param path The path to the transaction data.
   * @param updateTx The FamilyRevision to be updated.
   * @param mode The mode to update like append, update, remove.
   * @throws IOException
   */
  void updateData(String path, FamilyRevision updateTx, UpdateMode mode)
    throws IOException {

    if (updateTx == null) {
      throw new IOException(
        "The transaction to be updated found to be null.");
    }
    List<FamilyRevision> currentData = getTransactionList(path);
    List<FamilyRevision> newData = new ArrayList<FamilyRevision>();
    boolean dataFound = false;
    long updateVersion = updateTx.getRevision();
    for (FamilyRevision tranx : currentData) {
      if (tranx.getRevision() != updateVersion) {
        newData.add(tranx);
      } else {
        dataFound = true;
      }
    }
    switch (mode) {
    case REMOVE:
      if (dataFound == false) {
        throw new IOException(
          "The transaction to be removed not found in the data.");
      }
      LOG.info("Removed trasaction : " + updateTx.toString());
      break;
    case KEEP_ALIVE:
      if (dataFound == false) {
        throw new IOException(
          "The transaction to be kept alove not found in the data. It might have been expired.");
      }
      newData.add(updateTx);
      LOG.info("keep alive of transaction : " + updateTx.toString());
      break;
    case APPEND:
      if (dataFound == true) {
        throw new IOException(
          "The data to be appended already exists.");
      }
      newData.add(updateTx);
      LOG.info("Added transaction : " + updateTx.toString());
      break;
    }

    // For serialization purposes.
    List<StoreFamilyRevision> newTxnList = new ArrayList<StoreFamilyRevision>();
    for (FamilyRevision wtxn : newData) {
      StoreFamilyRevision newTxn = new StoreFamilyRevision(wtxn.getRevision(),
        wtxn.getExpireTimestamp());
      newTxnList.add(newTxn);
    }
    StoreFamilyRevisionList wtxnList = new StoreFamilyRevisionList(newTxnList);
    byte[] newByteData = serialize(wtxnList);

    Stat stat = null;
    try {
      stat = zkSession.setData(path, newByteData, -1);
    } catch (KeeperException e) {
      throw new IOException(
        "Exception while updating trasactional data. ", e);
    } catch (InterruptedException e) {
      throw new IOException(
        "Exception while updating trasactional data. ", e);
    }

    if (stat != null) {
      LOG.info("Transaction list stored at " + path + ".");
    }

  }

  /**
   * Refresh transactions on a given transaction data path.
   *
   * @param path The path to the transaction data.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void refreshTransactions(String path) throws IOException {
    List<FamilyRevision> currentData = getTransactionList(path);
    List<FamilyRevision> newData = new ArrayList<FamilyRevision>();

    for (FamilyRevision tranx : currentData) {
      if (tranx.getExpireTimestamp() > getTimeStamp()) {
        newData.add(tranx);
      }
    }

    if (newData.equals(currentData) == false) {
      List<StoreFamilyRevision> newTxnList = new ArrayList<StoreFamilyRevision>();
      for (FamilyRevision wtxn : newData) {
        StoreFamilyRevision newTxn = new StoreFamilyRevision(wtxn.getRevision(),
          wtxn.getExpireTimestamp());
        newTxnList.add(newTxn);
      }
      StoreFamilyRevisionList wtxnList = new StoreFamilyRevisionList(newTxnList);
      byte[] newByteData = serialize(wtxnList);

      try {
        zkSession.setData(path, newByteData, -1);
      } catch (KeeperException e) {
        throw new IOException(
          "Exception while updating trasactional data. ", e);
      } catch (InterruptedException e) {
        throw new IOException(
          "Exception while updating trasactional data. ", e);
      }

    }

  }

  /**
   * Delete table znodes.
   *
   * @param tableName the hbase table name
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void deleteZNodes(String tableName) throws IOException {
    String transactionDataTablePath = PathUtil.getTxnDataPath(baseDir,
      tableName);
    deleteRecursively(transactionDataTablePath);
  }

  void deleteRecursively(String path) throws IOException {
    try {
      List<String> children = getSession().getChildren(path, false);
      if (children.size() != 0) {
        for (String child : children) {
          deleteRecursively(path + "/" + child);
        }
      }
      getSession().delete(path, -1);
    } catch (KeeperException e) {
      throw new IOException(
        "Exception while deleting path " + path + ".", e);
    } catch (InterruptedException e) {
      throw new IOException(
        "Exception while deleting path " + path + ".", e);
    }
  }

  /**
   * This method serializes a given instance of TBase object.
   *
   * @param obj An instance of TBase
   * @return byte array The serialized data.
   * @throws IOException
   */
  static byte[] serialize(TBase obj) throws IOException {
    if (obj == null)
      return new byte[0];
    try {
      TSerializer serializer = new TSerializer(
        new TBinaryProtocol.Factory());
      byte[] bytes = serializer.serialize(obj);
      return bytes;
    } catch (Exception e) {
      throw new IOException("Serialization error: ", e);
    }
  }


  /**
   * This method deserializes the given byte array into the TBase object.
   *
   * @param obj An instance of TBase
   * @param data Output of deserialization.
   * @throws IOException
   */
  static void deserialize(TBase obj, byte[] data) throws IOException {
    if (data == null || data.length == 0)
      return;
    try {
      TDeserializer deserializer = new TDeserializer(
        new TBinaryProtocol.Factory());
      deserializer.deserialize(obj, data);
    } catch (Exception e) {
      throw new IOException("Deserialization error: " + e.getMessage(), e);
    }
  }

  private class ZKWatcher implements Watcher {
    public void process(WatchedEvent event) {
      switch (event.getState()) {
      case Expired:
        LOG.info("The client session has expired. Try opening a new "
          + "session and connecting again.");
        zkSession = null;
        break;
      default:

      }
    }
  }

}
