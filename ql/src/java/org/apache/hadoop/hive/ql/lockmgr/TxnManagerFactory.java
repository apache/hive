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
package org.apache.hadoop.hive.ql.lockmgr;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A factory to get an instance of {@link HiveTxnManager}.  This should
 * always be called rather than building a transaction manager via reflection.
 * This factory will read the configuration file to determine which
 * transaction manager to instantiate.  It will stash the chosen transaction
 * manager into the Context object, and subsequently return it from there so
 * that if there are multiple Hive threads running,
 * each will get it's appropriate transaction manager.
 */
public class TxnManagerFactory {

  private static TxnManagerFactory self;

  /**
   * Get the singleton instance of this factory.
   * @return this factory
   */
  public static synchronized TxnManagerFactory getTxnManagerFactory() {
    if (self == null) {
      self = new TxnManagerFactory();
    }
    return self;
  }

  /**
   * Create a new transaction manager.  The transaction manager to
   * instantiate will be determined by the hive.txn.manager value in the
   * configuration.  This should not be called if a transaction manager has
   * already been constructed and stored in the Context object.
   * @param conf HiveConf object used to construct the transaction manager
   * @return the transaction manager
   * @throws LockException if there is an error constructing the transaction
   * manager.
   */
  public HiveTxnManager getTxnManager(HiveConf conf) throws
      LockException {
    HiveTxnManager txnMgr = null;

    // Determine the transaction manager to use from the configuration.
    String txnMgrName = conf.getVar(HiveConf.ConfVars.HIVE_TXN_MANAGER);
    if (txnMgrName == null || txnMgrName.isEmpty()) {
      throw new LockException(ErrorMsg.TXNMGR_NOT_SPECIFIED.getMsg());
    }

    // Instantiate the chosen transaction manager
    try {
      HiveTxnManagerImpl impl = (HiveTxnManagerImpl)ReflectionUtils.newInstance(
            conf.getClassByName(txnMgrName), conf);
      impl.setHiveConf(conf);
      txnMgr = impl;
    } catch (ClassNotFoundException e) {
      throw new LockException(ErrorMsg.TXNMGR_NOT_INSTANTIATED.getMsg());
    }
    return txnMgr;
  }

  private TxnManagerFactory() {
  }
}
