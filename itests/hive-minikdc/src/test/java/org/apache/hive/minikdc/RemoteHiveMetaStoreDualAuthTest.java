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
import org.apache.hadoop.hive.metastore.TestRemoteHiveMetaStore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.Before;

public class RemoteHiveMetaStoreDualAuthTest extends TestRemoteHiveMetaStore {
  protected static String correctUser = "correct_user";
  protected static String correctPassword = "correct_passwd";
  protected static String wrongUser = "wrong_user";
  protected static String wrongPassword = "wrong_password";
  private static MiniHiveKdc miniKDC = null;
  protected static Configuration clientConf;
  protected static String hiveMetastorePrincipal;
  protected static String hiveMetastoreKeytab;
  protected static String wrongKeytab;
  protected static String wrongPrincipal;

  @Before
  public void setUp() throws Exception {
    if (null == miniKDC) {
      miniKDC = new MiniHiveKdc();
      hiveMetastorePrincipal =
              miniKDC.getFullyQualifiedServicePrincipal(miniKDC.getHiveMetastoreServicePrincipal());
      hiveMetastoreKeytab = miniKDC.getKeyTabFile(
              miniKDC.getServicePrincipalForUser(miniKDC.getHiveMetastoreServicePrincipal()));
      wrongKeytab = miniKDC.getKeyTabFile(MiniHiveKdc.HIVE_TEST_USER_2);
      // We don't expect wrongUser to be part of KDC
      wrongPrincipal = miniKDC.getFullyQualifiedServicePrincipal(wrongUser);

      initConf();
      MetastoreConf.setBoolVar(conf, ConfVars.EXECUTE_SET_UGI, false);
      clientConf = new Configuration(conf);

      MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION, "CONFIG");
      MetastoreConf.setVar(conf, ConfVars.THRIFT_AUTH_CONFIG_USERNAME, correctUser);
      MetastoreConf.setVar(conf, ConfVars.THRIFT_AUTH_CONFIG_PASSWORD, correctPassword);
      MetastoreConf.setBoolVar(conf, ConfVars.USE_THRIFT_SASL, true);
      MetastoreConf.setVar(conf, ConfVars.KERBEROS_PRINCIPAL, hiveMetastorePrincipal);
      MetastoreConf.setVar(conf, ConfVars.KERBEROS_KEYTAB_FILE, hiveMetastoreKeytab);
    }
    super.setUp();
  }
}
