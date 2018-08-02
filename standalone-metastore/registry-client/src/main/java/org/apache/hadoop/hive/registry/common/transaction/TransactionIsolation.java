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
package org.apache.hadoop.hive.registry.common.transaction;

import java.sql.Connection;

public enum TransactionIsolation {
    // Set default isolation level as recommended by the JDBC driver.
    // When used inside a nested transaction retains current transaction isolation.
    DEFAULT(-1),
    // Commenting out below isolation level as Oracle doesn't support it.
    //READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
    READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
    // Commenting out below isolation level as Oracle doesn't support it.
    // REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
    SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

    int value;

    TransactionIsolation(int isolationValue) {
        this.value = isolationValue;
    }

    public int getValue() {
        return value;
    }
}
