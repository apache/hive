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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.BeforeClass;

/**
 * Runs the tests defined in TestJdbcWithMiniKdc when DBTokenStore
 * is configured and HMS is setup in a remote secure mode and
 * impersonation is turned OFF
 */
public class TestJdbcWithDBTokenStoreNoDoAs extends TestJdbcWithMiniKdc{

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    confOverlay.put(ConfVars.HIVE_SERVER2_SESSION_HOOK.varname,
        SessionHookTest.class.getName());
    confOverlay.put(ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_ENABLED.varname, "false");

    miniHiveKdc = new MiniHiveKdc();
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(ConfVars.METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_CLS, "org.apache.hadoop.hive.thrift.DBTokenStore");
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    miniHS2 = MiniHiveKdc.getMiniHS2WithKerbWithRemoteHMSWithKerb(miniHiveKdc, hiveConf);
    miniHS2.start(confOverlay);
    String metastorePrincipal = miniHS2.getConfProperty(ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname);
    String hs2Principal = miniHS2.getConfProperty(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL.varname);
    String hs2KeyTab = miniHS2.getConfProperty(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB.varname);
    System.out.println("HS2 principal : " + hs2Principal + " HS2 keytab : " + hs2KeyTab + " Metastore principal : " + metastorePrincipal);
    System.setProperty(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname,
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.WAREHOUSE));
    System.setProperty(HiveConf.ConfVars.METASTORE_CONNECT_URL_KEY.varname,
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.CONNECT_URL_KEY));
    System.setProperty(HiveConf.ConfVars.METASTORE_URIS.varname,
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.THRIFT_URIS));
    System.setProperty(ConfVars.METASTORE_USE_THRIFT_SASL.varname,
        String.valueOf(MetastoreConf.getBoolVar(hiveConf, MetastoreConf.ConfVars.USE_THRIFT_SASL)));
    System.setProperty(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.KERBEROS_PRINCIPAL));
    System.setProperty(ConfVars.METASTORE_KERBEROS_KEYTAB_FILE.varname,
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.KERBEROS_KEYTAB_FILE));
  }
}