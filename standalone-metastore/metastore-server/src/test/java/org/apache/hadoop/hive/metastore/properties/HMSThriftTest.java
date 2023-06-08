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
package org.apache.hadoop.hive.metastore.properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class HMSThriftTest extends HMSTestBase {
  /**
   * A Thrift based property client.
   */
  static class ThriftPropertyClient implements PropertyClient {
    private final String namespace;
    private final HiveMetaStoreClient client;
    ThriftPropertyClient(String ns, HiveMetaStoreClient c) {
      this.namespace = ns;
      this.client = c;
    }

    @Override
    public boolean setProperties(Map<String, String> properties) {
      try {
        return client.setProperties(namespace, properties);
      } catch(TException tex) {
        return false;
      }
    }

    @Override
    public Map<String, Map<String, String>> getProperties(String mapPrefix, String mapPredicate, String... selection) throws IOException {
      try {
        return client.getProperties(namespace, mapPrefix, mapPredicate, selection);
      } catch(TException tex) {
        return null;
      }
    }
  }

  @Override protected int createServer(Configuration conf) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
  }

  @Override protected void stopServer(int port) {
    MetaStoreTestUtils.close(port);
  }

  /**
   * Creates a client.
   * @return the client instance
   * @throws Exception
   */
  @Override protected PropertyClient createClient(Configuration conf, int port) throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, "http://localhost:" + port);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI, false);
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    return new ThriftPropertyClient(NS, client);
  }

  @Test
  public void testThriftProperties0() throws Exception {
    runOtherProperties0(client);
  }

  @Test
  public void testThriftProperties1() throws Exception {
    runOtherProperties1(client);
  }

}
