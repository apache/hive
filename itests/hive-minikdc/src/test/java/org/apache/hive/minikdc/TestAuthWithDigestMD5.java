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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TestAuthWithDigestMD5 {

    private static final Logger LOG = LoggerFactory.getLogger(TestAuthWithDigestMD5.class.getName());
    protected static HiveMetaStoreClient client;
    public static final String HS2TOKEN = "HiveServer2ImpersonationToken";

    protected static String correctUser = "correct_user";
    protected static String correctPassword = "correct_passwd";

    protected static Configuration conf = null;

    private static MiniHiveKdc miniKDC = null;
    protected static Configuration clientConf;
    protected static String hiveMetastorePrincipal;
    protected static String hiveMetastoreKeytab;
    private static boolean isServerStarted = false;
    protected static int port;

    @Before
    public void setUp() throws Exception {
        miniKDC = new MiniHiveKdc();
        hiveMetastorePrincipal =
                miniKDC.getFullyQualifiedServicePrincipal(miniKDC.getHiveMetastoreServicePrincipal());
        hiveMetastoreKeytab = miniKDC.getKeyTabFile(
                miniKDC.getServicePrincipalForUser(miniKDC.getHiveMetastoreServicePrincipal()));

        if (null == conf) {
            conf = MetastoreConf.newMetastoreConf();
        }

        MetastoreConf.setBoolVar(conf, ConfVars.EXECUTE_SET_UGI, false);
        clientConf = new Configuration(conf);

        MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION, "CONFIG");
        MetastoreConf.setVar(conf, ConfVars.THRIFT_AUTH_CONFIG_USERNAME, correctUser);
        MetastoreConf.setVar(conf, ConfVars.THRIFT_AUTH_CONFIG_PASSWORD, correctPassword);
        MetastoreConf.setBoolVar(conf, ConfVars.USE_THRIFT_SASL, true);
        MetastoreConf.setVar(conf, ConfVars.KERBEROS_PRINCIPAL, hiveMetastorePrincipal);
        MetastoreConf.setVar(conf, ConfVars.KERBEROS_KEYTAB_FILE, hiveMetastoreKeytab);

        // set some values to use for getting conf. vars
        MetastoreConf.setBoolVar(conf, ConfVars.METRICS_ENABLED, true);
        conf.set("datanucleus.autoCreateTables", "false");
        conf.set("hive.in.test", "true");
        MetastoreConf.setVar(conf, ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS, " ");

        MetaStoreTestUtils.setConfForStandloneMode(conf);
        MetastoreConf.setLongVar(conf, ConfVars.BATCH_RETRIEVE_MAX, 2);
        MetastoreConf.setVar(conf, ConfVars.STORAGE_SCHEMA_READER_IMPL, "no.such.class");
        MetastoreConf.setBoolVar(conf, ConfVars.INTEGER_JDO_PUSHDOWN, true);
        MetastoreConf.setTimeVar(conf, ConfVars.DELEGATION_TOKEN_RENEW_INTERVAL,10000, TimeUnit.MILLISECONDS);
        MetastoreConf.setTimeVar(conf, ConfVars.DELEGATION_TOKEN_GC_INTERVAL,3000, TimeUnit.MILLISECONDS);

        if (isServerStarted) {
            Assert.assertNotNull("Unable to connect to the MetaStore server", client);
            return;
        }
        start();
    }

    protected void start() throws Exception {
        port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(),
                conf);
        System.out.println("Starting MetaStore Server on port " + port);
        isServerStarted = true;

        MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
        MetastoreConf.setBoolVar(conf, ConfVars.EXECUTE_SET_UGI, false);

        client = createClient();
    }

    protected HiveMetaStoreClient createClient() throws Exception {
        MetastoreConf.setVar(clientConf, ConfVars.KERBEROS_PRINCIPAL, hiveMetastorePrincipal);
        MetastoreConf.setVar(clientConf, ConfVars.KERBEROS_KEYTAB_FILE, hiveMetastoreKeytab);
        return new HiveMetaStoreClient(conf);
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

        LOG.info("Sleeping for 15 seconds");
        Thread.sleep(15000);
        LOG.info("Waking Up");

        client = new HiveMetaStoreClient(conf);
    }

}
