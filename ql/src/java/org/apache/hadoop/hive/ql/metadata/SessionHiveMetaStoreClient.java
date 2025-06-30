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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.HookEnabledMetaStoreClientProxy;
import org.apache.hadoop.hive.metastore.client.BaseMetaStoreClientProxy;
import org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.client.LocalCachingMetaStoreClientProxy;
import org.apache.hadoop.hive.ql.metadata.client.SessionMetaStoreClientProxy;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.Map;

/**
 * todo: This need review re: thread safety.  Various places (see callsers of
 * {@link SessionState#setCurrentSessionState(SessionState)}) pass SessionState to forked threads.
 * Currently it looks like those threads only read metadata but this is fragile.
 * Also, maps (in SessionState) where tempt table metadata is stored are concurrent and so
 * any put/get crosses a memory barrier and so does using most {@code java.util.concurrent.*}
 * so the readers of the objects in these maps should have the most recent view of the object.
 * But again, could be fragile.
 */
public class SessionHiveMetaStoreClient extends BaseMetaStoreClientProxy {
  private SessionHiveMetaStoreClient(
      Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
    super(createUnderlyingClient(conf, hookLoader, allowEmbedded), conf);
  }

  static SessionHiveMetaStoreClient newClient(
      Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
    return new SessionHiveMetaStoreClient(conf, hookLoader, allowEmbedded);
  }

  @SuppressWarnings("squid:S2095")
  private static IMetaStoreClient createUnderlyingClient(Configuration conf, HiveMetaHookLoader hookLoader,
      Boolean allowEmbedded) throws MetaException {
    IMetaStoreClient thriftClient = new ThriftHiveMetaStoreClient(conf, allowEmbedded);
    IMetaStoreClient clientWithLocalCache = new LocalCachingMetaStoreClientProxy(conf, thriftClient);
    IMetaStoreClient sessionLevelClient = new SessionMetaStoreClientProxy(conf, clientWithLocalCache);
    IMetaStoreClient clientWithHook = new HookEnabledMetaStoreClientProxy(conf, hookLoader, sessionLevelClient);
    return clientWithHook;
  }

  public static Map<String, Table> getTempTablesForDatabase(String dbName, String tblName) {
    return SessionMetaStoreClientProxy.getTempTablesForDatabase(dbName, tblName);
  }
}
