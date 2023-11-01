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

package org.apache.hadoop.hive.ql.session;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TestRemoteHiveMetaStore;
import org.apache.hadoop.hive.metastore.TestHiveMetaStore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.thrift.transport.TTransportException;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.minikdc.MiniHiveKdc;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;


import java.util.List;

/**
 * TestAddMetastoreDelegationTokenToUGI : Simple base test to add metastore delegation token to UserGroupInformation object
 */
public class TestAddMetastoreDelegationTokenToUGI extends TestRemoteHiveMetaStore {

  private static final String CLITOKEN = "HiveClientImpersonationToken";
  public static MiniHiveKdc miniKDC;

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
  public void testGetDelegationTokenFromUGI() throws Exception {
    HiveConf clientConf = new HiveConf(conf, TestAddMetastoreDelegationTokenToUGI.class);
    HiveConf.setVar(clientConf, HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    HiveConf.setBoolVar(clientConf, HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, true);
    SessionState.start(new CliSessionState(clientConf));
    assertNotNull(SecurityUtils.getTokenStrForm(CLITOKEN));
  }

}
