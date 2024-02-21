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

package org.apache.hadoop.hive.ql.security;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * TestMetastoreClientSideAuthorizationProvider : Simple base test for Metastore client side
 * Authorization Providers. By default, tests DefaultHiveAuthorizationProvider
 */
public class TestMetastoreClientSideAuthorizationProvider {
    private HiveConf clientHiveConf;
    private HiveMetaStoreClient msc;
    private IDriver driver;
    private UserGroupInformation ugi;

    @Before
    public void setUp() throws Exception {
        System.setProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
                "org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener");

        int port = MetaStoreTestUtils.startMetaStoreWithRetry();

        clientHiveConf = new HiveConf(this.getClass());

        // Turn on client-side authorization
        clientHiveConf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED,true);
        clientHiveConf.set(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.varname,
                getAuthorizationProvider());
        clientHiveConf.set(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER.varname,
                InjectableDummyAuthenticator.class.getName());
        clientHiveConf.set(HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_OWNER_GRANTS.varname, "");
        clientHiveConf.setVar(HiveConf.ConfVars.HIVE_MAPRED_MODE, "nonstrict");
        clientHiveConf.setVar(HiveConf.ConfVars.METASTORE_URIS, "thrift://localhost:" + port);
        clientHiveConf.setIntVar(HiveConf.ConfVars.METASTORE_THRIFT_CONNECTION_RETRIES, 3);
        clientHiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

        clientHiveConf.set(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, "");
        clientHiveConf.set(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, "");

        ugi = Utils.getUGI();

        SessionState.start(new CliSessionState(clientHiveConf));
        msc = new HiveMetaStoreClient(clientHiveConf);
        driver = DriverFactory.newDriver(clientHiveConf);
    }

    private String getAuthorizationProvider(){
        return DefaultHiveAuthorizationProvider.class.getName();
    }

    private void allowCreateDatabase(String userName) throws Exception {
        driver.run("grant create to user " + userName);
    }

    private void allowSelect(String userName) throws Exception {
        driver.run("grant select to user " + userName);
    }

    private void revokeSelect(String userName) throws Exception {
        driver.run("revoke select from user " + userName);
    }

    protected void allowCreateInDb(String dbName, String userName) throws Exception {
        driver.run("grant create on database "+ dbName +" to user " + userName);
    }

    private void allowSelectInDb(String dbName, String userName) throws Exception {
        driver.run("grant select on database " + dbName + " to user " + userName);
    }


    private void disallowSelectDatabase(String dbName, String userName) throws Exception {
        driver.run("revoke select on database " + dbName + " from user " + userName);
    }

    @Test
    public void testGetTableMeta() throws Exception {
        String dbName1 = "database1";
        String tblName1 = "table1";

        String userName = ugi.getUserName();
        allowCreateDatabase(userName);
        allowSelect(userName);
        driver.run("create database " + dbName1);
        allowCreateInDb(dbName1, userName);
        driver.run("use " + dbName1);
        driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName1));
        allowSelectInDb(dbName1, userName);

        String dbName2 = "database2";
        String tblName2 = "table2";
        driver.run("create database " + dbName2);
        allowSelectInDb(dbName2, userName);
        driver.run("use " + dbName2);
        driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName2));

        List<TableMeta> tableMetas = msc.getTableMeta("*", "*", Lists.newArrayList());
        assertEquals(2, tableMetas.size());

        revokeSelect(userName);
        disallowSelectDatabase(dbName1, userName);
        tableMetas = msc.getTableMeta("*", "*", Lists.newArrayList());
        assertEquals(1, tableMetas.size());
        assertEquals(dbName2, tableMetas.get(0).getDbName());
        assertEquals(tblName2, tableMetas.get(0).getTableName());

        disallowSelectDatabase(dbName2, userName);
        tableMetas = msc.getTableMeta("*", "*", Lists.newArrayList());
        assertEquals(0, tableMetas.size());
    }
}
