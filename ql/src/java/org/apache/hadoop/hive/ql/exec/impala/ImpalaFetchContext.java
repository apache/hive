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
package org.apache.hadoop.hive.ql.exec.impala;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TRowSet;

/**
 * Context with information required to stream results from an Impala coordinator.
 */
public class ImpalaFetchContext {
    /* Session that submitted query to Impala coordinator */
    private final ImpalaSession session;
    /* Operation handled associated with requested execution mode */
    private TOperationHandle operationHandle;
    /* Desired fetch size */
    private final long fetchSize;

    ImpalaFetchContext(ImpalaSession session, TOperationHandle operationHandle, long fetchSize) {
        Preconditions.checkNotNull(session);
        Preconditions.checkNotNull(operationHandle);
        Preconditions.checkArgument(fetchSize > 0);
        this.session = session;
        this.operationHandle = operationHandle;
        this.fetchSize = fetchSize;
    }

    public TRowSet fetch() throws HiveException {
        return session.fetch(operationHandle, fetchSize);
    }

    public void close() throws HiveException {
        if (operationHandle != null) {
            session.closeOperation(operationHandle);
            operationHandle = null;
        }
    }
}
