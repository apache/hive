/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.registry.storage.core.transaction;

import java.sql.Connection;

public class TransactionContext {
    private int nestedTransactionCount = 1;
    private final Connection connection;
    private int transactionState = TransactionState.INITIALIZED.value;

    public TransactionContext(Connection connection) {
        this.connection = connection;
    }

    public void incrementNestedTransactionCount() {
        this.nestedTransactionCount++;
    }

    public void decrementNestedTransactionCount() {
        this.nestedTransactionCount--;
    }

    public Connection getConnection() {
        return this.connection;
    }

    public int getNestedTransactionCount() {
        return this.nestedTransactionCount;
    }

    public int getTransactionState() {
        return this.transactionState;
    }

    public void recordState(TransactionState state) {
        transactionState |= state.value;
    }
}
