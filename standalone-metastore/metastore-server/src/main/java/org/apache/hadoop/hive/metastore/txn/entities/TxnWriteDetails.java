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
package org.apache.hadoop.hive.metastore.txn.entities;

/**
 * Class to hold transaction id, database name and write id.
 */
public class TxnWriteDetails {
    private final long txnId;
    private final String dbName;
    private final long writeId;

    public TxnWriteDetails(long txnId, String dbName, long writeId) {
        this.txnId = txnId;
        this.dbName = dbName;
        this.writeId = writeId;
    }

    @Override
    public String toString() {
        return "TxnToWriteID{" +
                "txnId=" + txnId +
                ", dbName='" + dbName + '\'' +
                ", writeId=" + writeId +
                '}';
    }

    public long getTxnId() {
        return txnId;
    }

    public String getDbName() {
        return dbName;
    }

    public long getWriteId() {
        return writeId;
    }
}