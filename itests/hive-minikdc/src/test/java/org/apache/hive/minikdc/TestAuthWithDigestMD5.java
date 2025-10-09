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
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.DelegationTokenIdentifier;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestAuthWithDigestMD5 {

    MiniHiveKdc miniKDC = null;
    private static HiveMetaStoreClient client;
    private static final String HS2TOKEN = "HiveServer2ImpersonationToken";
    private static Configuration conf = null;
    private static int port;

    @Before
    public void setUp() throws Exception {
        miniKDC = new MiniHiveKdc();
        String hiveMetastorePrincipal = miniKDC.getFullyQualifiedServicePrincipal(miniKDC.getHiveMetastoreServicePrincipal());
        String hiveMetastoreKeytab = miniKDC.getKeyTabFile(
                miniKDC.getServicePrincipalForUser(miniKDC.getHiveMetastoreServicePrincipal()));
        conf = MetastoreConf.newMetastoreConf();

        String correctUser = "correct_user";
        String correctPassword = "correct_passwd";

        MetastoreConf.setBoolVar(conf, ConfVars.EXECUTE_SET_UGI, false);
        MetastoreConf.setVar(conf, ConfVars.THRIFT_AUTH_CONFIG_USERNAME, correctUser);
        MetastoreConf.setVar(conf, ConfVars.THRIFT_AUTH_CONFIG_PASSWORD, correctPassword);
        MetastoreConf.setBoolVar(conf, ConfVars.USE_THRIFT_SASL, true);
        MetastoreConf.setVar(conf, ConfVars.KERBEROS_PRINCIPAL, hiveMetastorePrincipal);
        MetastoreConf.setVar(conf, ConfVars.KERBEROS_KEYTAB_FILE, hiveMetastoreKeytab);

        MetastoreConf.setTimeVar(conf, ConfVars.DELEGATION_TOKEN_RENEW_INTERVAL,2000, TimeUnit.MILLISECONDS);
        MetastoreConf.setTimeVar(conf, ConfVars.DELEGATION_TOKEN_GC_INTERVAL,1000, TimeUnit.MILLISECONDS);

        port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(),
                conf);
        System.out.println("Starting MetaStore Server on port " + port);

        MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
        client = new HiveMetaStoreClient(conf);
    }


    @Test
    public void testPostRenewalTimeThreadKickedIn() throws Exception {
        String token = client.getDelegationToken("hive","hive");
        MetastoreConf.setVar(conf, ConfVars.TOKEN_SIGNATURE, HS2TOKEN);

        client = new HiveMetaStoreClient(conf);

        Token<DelegationTokenIdentifier> delegationToken = new Token<DelegationTokenIdentifier>();
        delegationToken.decodeFromUrlString(token);
        delegationToken.setService(new Text(HS2TOKEN));
        UserGroupInformation.getCurrentUser().addToken(delegationToken);

        Thread.sleep(4000);
        client = new HiveMetaStoreClient(conf);
        client.close();
    }

    @After
    public void tearDown() {
        MetaStoreTestUtils.close(port);
        miniKDC.shutDown();
    }

}
