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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of RevisionManager as HBase RPC endpoint. This class will control the lifecycle of
 * and delegate to the actual RevisionManager implementation and make it available as a service
 * hosted in the HBase region server (instead of running it in the client (storage handler).
 * In the case of {@link ZKBasedRevisionManager} now only the region servers need write access to
 * manage revision data.
 */
public class RevisionManagerEndpoint extends BaseEndpointCoprocessor implements RevisionManagerProtocol {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(RevisionManagerEndpoint.class.getName());

  private RevisionManager rmImpl = null;

  @Override
  public void start(CoprocessorEnvironment env) {
    super.start(env);
    try {
      Configuration conf = RevisionManagerConfiguration.create(env.getConfiguration());
      String className = conf.get(RMConstants.REVISION_MGR_ENDPOINT_IMPL_CLASS,
        ZKBasedRevisionManager.class.getName());
      LOGGER.debug("Using Revision Manager implementation: {}", className);
      rmImpl = RevisionManagerFactory.getOpenedRevisionManager(className, conf);
    } catch (IOException e) {
      LOGGER.error("Failed to initialize revision manager", e);
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (rmImpl != null) {
      try {
        rmImpl.close();
      } catch (IOException e) {
        LOGGER.warn("Error closing revision manager.", e);
      }
    }
    super.stop(env);
  }

  @Override
  public void initialize(Configuration conf) {
    // do nothing, HBase controls life cycle
  }

  @Override
  public void open() throws IOException {
    // do nothing, HBase controls life cycle
  }

  @Override
  public void close() throws IOException {
    // do nothing, HBase controls life cycle
  }

  @Override
  public void createTable(String table, List<String> columnFamilies) throws IOException {
    rmImpl.createTable(table, columnFamilies);
  }

  @Override
  public void dropTable(String table) throws IOException {
    rmImpl.dropTable(table);
  }

  @Override
  public Transaction beginWriteTransaction(String table, List<String> families)
    throws IOException {
    return rmImpl.beginWriteTransaction(table, families);
  }

  @Override
  public Transaction beginWriteTransaction(String table,
                       List<String> families, long keepAlive) throws IOException {
    return rmImpl.beginWriteTransaction(table, families, keepAlive);
  }

  @Override
  public void commitWriteTransaction(Transaction transaction)
    throws IOException {
    rmImpl.commitWriteTransaction(transaction);
  }

  @Override
  public void abortWriteTransaction(Transaction transaction)
    throws IOException {
    rmImpl.abortWriteTransaction(transaction);
  }

  @Override
  public TableSnapshot createSnapshot(String tableName) throws IOException {
    return rmImpl.createSnapshot(tableName);
  }

  @Override
  public TableSnapshot createSnapshot(String tableName, long revision)
    throws IOException {
    return rmImpl.createSnapshot(tableName, revision);
  }

  @Override
  public void keepAlive(Transaction transaction) throws IOException {
    rmImpl.keepAlive(transaction);
  }

  @Override
  public List<FamilyRevision> getAbortedWriteTransactions(String table,
                              String columnFamily) throws IOException {
    return rmImpl.getAbortedWriteTransactions(table, columnFamily);
  }

}
