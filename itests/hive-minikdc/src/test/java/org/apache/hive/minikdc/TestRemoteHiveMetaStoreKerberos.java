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

package org.apache.hive.minikdc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TestRemoteHiveMetaStore;
import org.apache.hadoop.hive.metastore.TestHiveMetaStore;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestRemoteHiveMetaStoreKerberos extends TestRemoteHiveMetaStore {
  private static MiniHiveKdc miniKDC;

  @Before
  public void setUp() throws Exception {
    if (null == miniKDC) {
      miniKDC = new MiniHiveKdc();
      String hiveMetastorePrincipal =
              miniKDC.getFullyQualifiedServicePrincipal(miniKDC.getHiveMetastoreServicePrincipal());
      String hiveMetastoreKeytab = miniKDC.getKeyTabFile(
              miniKDC.getServicePrincipalForUser(miniKDC.getHiveMetastoreServicePrincipal()));

      initConf();
      MetastoreConf.setBoolVar(conf, ConfVars.USE_THRIFT_SASL, true);
      MetastoreConf.setVar(conf, ConfVars.KERBEROS_PRINCIPAL, hiveMetastorePrincipal);
      MetastoreConf.setVar(conf, ConfVars.KERBEROS_KEYTAB_FILE, hiveMetastoreKeytab);
      MetastoreConf.setBoolVar(conf, ConfVars.EXECUTE_SET_UGI, false);
    }
    super.setUp();
  }

  @Test
  public void testThriftMaxMessageSize() throws Throwable {
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";

    cleanUp(dbName, tblName, typeName);
    List<List<String>> values = new ArrayList<>();
    values.add(makeVals("2008-07-01 14:13:12", "14"));
    values.add(makeVals("2008-07-01 14:13:12", "15"));
    values.add(makeVals("2008-07-02 14:13:12", "15"));
    values.add(makeVals("2008-07-03 14:13:12", "151"));

    createMultiPartitionTableSchema(dbName, tblName, typeName, values);

    Configuration clientConf = MetastoreConf.newMetastoreConf(new Configuration(conf));
    MetastoreConf.setVar(clientConf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    // set to a low value to prove THRIFT_METASTORE_CLIENT_MAX_MESSAGE_SIZE is being honored
    // (it should throw an exception)
    MetastoreConf.setVar(clientConf, ConfVars.THRIFT_METASTORE_CLIENT_MAX_MESSAGE_SIZE, "1024");
    HiveMetaStoreClient limitedClient = new HiveMetaStoreClient(clientConf);
    Exception expectedException = assertThrows(TTransportException.class, () -> {
      limitedClient.listPartitions(dbName, tblName, (short)-1);
    });
    String exceptionMessage = expectedException.getMessage();
    // Verify the Thrift library is enforcing the limit
    assertTrue(exceptionMessage.contains("MaxMessageSize reached"));
    limitedClient.close();

    // test default client (with a default THRIFT_METASTORE_CLIENT_MAX_MESSAGE_SIZE)
    List<Partition> partitions = client.listPartitions(dbName, tblName, (short) -1);
    assertNotNull(partitions);
    assertEquals("expected to receive the same number of partitions added", values.size(), partitions.size());

    cleanUp(dbName, tblName, typeName);
  }

  @Override
  protected HiveMetaStoreClient createClient() throws Exception {
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    return new HiveMetaStoreClient(conf);
  }
}
