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

public interface TransactionManager {

    /**
     * Begins the transaction
     */
    void beginTransaction(TransactionIsolation transactionIsolationLevel);


    /**
     * Discards the changes made to the storage layer and reverts to the last committed point.
     *
     * Implementation of this method must guarantee transaction is closed after calling this method,
     * whether it throws Exception or not, because without guarantee, caller should try calling
     * this method infinitely unless it succeeds, to avoid transaction left in open and possibly be leaked.
     *
     * With this guarantee, caller can just call a method once (and optionally handles Exception)
     * without worrying about the status of transaction.
     */
    void rollbackTransaction();


    /**
     * Flushes the changes made to the storage layer.
     *
     * Unlike of rollbackTransaction(), implementation of this method doesn't need to guarantee
     * transaction is closed, since in normal caller would want to call rollbackTransaction()
     * when commitTransaction() throws Exception.
     */
    void commitTransaction();
}
