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
package org.apache.hadoop.hive.metastore.hbase;

import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.distributed.ThreadLocalClientProvider;
import co.cask.tephra.distributed.TransactionServiceClient;
import co.cask.tephra.hbase10.TransactionAwareHTable;
import co.cask.tephra.hbase10.coprocessor.TransactionProcessor;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.twill.discovery.InMemoryDiscoveryService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class that uses Tephra for transaction management.
 */
public class TephraHBaseConnection extends VanillaHBaseConnection {
  static final private Logger LOG = LoggerFactory.getLogger(TephraHBaseConnection.class.getName());

  private Map<String, TransactionAware> txnTables;
  private TransactionContext txn;
  private TransactionSystemClient txnClient;

  TephraHBaseConnection() {
    super();
    txnTables = new HashMap<String, TransactionAware>();
  }

  @Override
  public void connect() throws IOException {
    super.connect();
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEST)) {
      LOG.debug("Using an in memory client transaction system for testing");
      TransactionManager txnMgr = new TransactionManager(conf);
      txnMgr.startAndWait();
      txnClient = new InMemoryTxSystemClient(txnMgr);
    } else {
      // TODO should enable use of ZKDiscoveryService if users want it
      LOG.debug("Using real client transaction system for production");
      txnClient = new TransactionServiceClient(conf,
          new ThreadLocalClientProvider(conf, new InMemoryDiscoveryService()));
    }
    for (String tableName : HBaseReadWrite.tableNames) {
      txnTables.put(tableName, new TransactionAwareHTable(super.getHBaseTable(tableName, true)));
    }
    txn = new TransactionContext(txnClient, txnTables.values());
  }

  @Override
  public void beginTransaction() throws IOException {
    try {
      txn.start();
      LOG.debug("Started txn in tephra");
    } catch (TransactionFailureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void commitTransaction() throws IOException {
    try {
      txn.finish();
      LOG.debug("Finished txn in tephra");
    } catch (TransactionFailureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void rollbackTransaction() throws IOException {
    try {
      txn.abort();
      LOG.debug("Aborted txn in tephra");
    } catch (TransactionFailureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void flush(HTableInterface htab) throws IOException {
    // NO-OP as we want to flush at commit time
  }

  @Override
  protected HTableDescriptor buildDescriptor(String tableName, List<byte[]> columnFamilies)
      throws IOException {
    HTableDescriptor tableDesc = super.buildDescriptor(tableName, columnFamilies);
    tableDesc.addCoprocessor(TransactionProcessor.class.getName());
    return tableDesc;
  }

  @Override
  public HTableInterface getHBaseTable(String tableName, boolean force) throws IOException {
    // Ignore force, it will mess up our previous creation of the tables.
    return (TransactionAwareHTable)txnTables.get(tableName);
  }

}
