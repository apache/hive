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
package org.apache.hadoop.hive.registry.storage.core;

import org.apache.hadoop.hive.registry.common.transaction.TransactionIsolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NOOPTransactionManager implements TransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(NOOPTransactionManager.class);

    @Override
    public void beginTransaction(TransactionIsolation transactionIsolationLevel) {
        LOG.debug(String.format("--- Ignore call to begin transaction for thread id : %s ---", Long.toString(Thread.currentThread().getId())));
    }

    @Override
    public void rollbackTransaction() {
        LOG.debug(String.format("--- Ignore call to rollback transaction for thread id : %s ---", Long.toString(Thread.currentThread().getId())));
    }

    @Override
    public void commitTransaction() {
        LOG.debug(String.format("--- Ignore call to commit transaction for thread id : %s ---", Long.toString(Thread.currentThread().getId())));
    }
}
