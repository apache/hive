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
package org.apache.hadoop.hive.ql.txn.compactor.handler;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.txn.compactor.FSRemover;
import org.apache.hadoop.hive.ql.txn.compactor.MetadataCache;

import java.util.ArrayList;
import java.util.List;

/**
 * A factory class to fetch handlers.
 */
public class TaskHandlerFactory {
  private static final TaskHandlerFactory INSTANCE = new TaskHandlerFactory();

  public static TaskHandlerFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Factory class, no need to expose constructor.
   */
  private TaskHandlerFactory() {
  }

  public List<TaskHandler> getHandlers(HiveConf conf, TxnStore txnHandler, MetadataCache metadataCache,
                                                  boolean metricsEnabled, FSRemover fsRemover) {
    boolean useAbortHandler = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);
    List<TaskHandler> taskHandlers = new ArrayList<>();
    taskHandlers.add(new CompactionCleaner(conf, txnHandler, metadataCache,
            metricsEnabled, fsRemover));
    if (useAbortHandler) {
      taskHandlers.add(new AbortedTxnCleaner(conf, txnHandler, metadataCache,
              metricsEnabled, fsRemover));
    }
    return taskHandlers;
  }
}
