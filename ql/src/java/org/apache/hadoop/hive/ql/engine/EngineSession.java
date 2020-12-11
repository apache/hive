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
package org.apache.hadoop.hive.ql.engine;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TRowSet;

import java.util.Map;

/**
 * EngineSession.  This interface is used to create a session object that is
 * engine specific.
 */
@InterfaceStability.Unstable
public interface EngineSession {

    public void init(HiveConf conf);

    /* Given a valid TOperationHandle attempts to retrieve rows from the engine. */
    public TRowSet fetch(TOperationHandle opHandle, long fetchSize) throws HiveException;

    public void notifyShutdown();

    public void closeOperation(TOperationHandle opHandle) throws HiveException;

    /* Executes a query string */
    public TOperationHandle execute(String sql) throws HiveException;

    public boolean isOpen();

    /* Opens a session */
    public void open() throws HiveException;

    public THandleIdentifier getSessionId();

    public Map<String,String> getSessionConfig();

    /* Closes a session */
    public void close();
}
