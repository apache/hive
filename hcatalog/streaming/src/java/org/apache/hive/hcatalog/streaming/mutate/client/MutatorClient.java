/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.Lock;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.LockFailureListener;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for orchestrating {@link Transaction Transactions} within which ACID table mutation events can occur.
 * Typically this will be a large batch of delta operations.
 */
public class MutatorClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MutatorClient.class);
  private static final String TRANSACTIONAL_PARAM_KEY = "transactional";

  private final IMetaStoreClient metaStoreClient;
  private final Lock.Options lockOptions;
  private final List<AcidTable> tables;
  private boolean connected;

  MutatorClient(IMetaStoreClient metaStoreClient, HiveConf configuration, LockFailureListener lockFailureListener,
      String user, Collection<AcidTable> tables) {
    this.metaStoreClient = metaStoreClient;
    this.tables = Collections.unmodifiableList(new ArrayList<>(tables));

    lockOptions = new Lock.Options()
        .configuration(configuration)
        .lockFailureListener(lockFailureListener == null ? LockFailureListener.NULL_LISTENER : lockFailureListener)
        .user(user);
    for (AcidTable table : tables) {
      switch (table.getTableType()) {
      case SOURCE:
        lockOptions.addSourceTable(table.getDatabaseName(), table.getTableName());
        break;
      case SINK:
        lockOptions.addSinkTable(table.getDatabaseName(), table.getTableName());
        break;
      default:
        throw new IllegalArgumentException("Unknown TableType: " + table.getTableType());
      }
    }
  }

  /**
   * Connects to the {@link IMetaStoreClient meta store} that will be used to manage {@link Transaction} life-cycles.
   * Also checks that the tables destined to receive mutation events are able to do so. The client should only hold one
   * open transaction at any given time (TODO: enforce this).
   */
  public void connect() throws ConnectionException {
    if (connected) {
      throw new ConnectionException("Already connected.");
    }
    for (AcidTable table : tables) {
      checkTable(metaStoreClient, table);
    }
    LOG.debug("Connected to end point {}", metaStoreClient);
    connected = true;
  }

  /** Creates a new {@link Transaction} by opening a transaction with the {@link IMetaStoreClient meta store}. */
  public Transaction newTransaction() throws TransactionException {
    if (!connected) {
      throw new TransactionException("Not connected - cannot create transaction.");
    }
    Transaction transaction = new Transaction(metaStoreClient, lockOptions);
    for (AcidTable table : tables) {
      table.setTransactionId(transaction.getTransactionId());
    }
    LOG.debug("Created transaction {}", transaction);
    return transaction;
  }

  /** Did the client connect successfully. Note the the client may have since become disconnected. */
  public boolean isConnected() {
    return connected;
  }

  /**
   * Closes the client releasing any {@link IMetaStoreClient meta store} connections held. Does not notify any open
   * transactions (TODO: perhaps it should?)
   */
  @Override
  public void close() throws IOException {
    metaStoreClient.close();
    LOG.debug("Closed client.");
    connected = false;
  }

  /**
   * Returns the list of managed {@link AcidTable AcidTables} that can receive mutation events under the control of this
   * client.
   */
  public List<AcidTable> getTables() throws ConnectionException {
    if (!connected) {
      throw new ConnectionException("Not connected - cannot interrogate tables.");
    }
    return Collections.<AcidTable> unmodifiableList(tables);
  }

  @Override
  public String toString() {
    return "MutatorClient [metaStoreClient=" + metaStoreClient + ", connected=" + connected + "]";
  }

  private void checkTable(IMetaStoreClient metaStoreClient, AcidTable acidTable) throws ConnectionException {
    try {
      LOG.debug("Checking table {}.", acidTable.getQualifiedName());
      Table metaStoreTable = metaStoreClient.getTable(acidTable.getDatabaseName(), acidTable.getTableName());

      if (acidTable.getTableType() == TableType.SINK) {
        Map<String, String> parameters = metaStoreTable.getParameters();
        if (!Boolean.parseBoolean(parameters.get(TRANSACTIONAL_PARAM_KEY))) {
          throw new ConnectionException("Cannot stream to table that is not transactional: '"
              + acidTable.getQualifiedName() + "'.");
        }
        int totalBuckets = metaStoreTable.getSd().getNumBuckets();
        LOG.debug("Table {} has {} buckets.", acidTable.getQualifiedName(), totalBuckets);
        if (totalBuckets <= 0) {
          throw new ConnectionException("Cannot stream to table that has not been bucketed: '"
              + acidTable.getQualifiedName() + "'.");
        }

        String outputFormat = metaStoreTable.getSd().getOutputFormat();
        LOG.debug("Table {} has {} OutputFormat.", acidTable.getQualifiedName(), outputFormat);
        acidTable.setTable(metaStoreTable);
      }
    } catch (NoSuchObjectException e) {
      throw new ConnectionException("Invalid table '" + acidTable.getQualifiedName() + "'", e);
    } catch (TException e) {
      throw new ConnectionException("Error communicating with the meta store", e);
    }
    LOG.debug("Table {} OK.", acidTable.getQualifiedName());
  }

}
