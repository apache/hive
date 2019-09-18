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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.Assert;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import javax.security.sasl.AuthenticationException;
import java.util.HashMap;
import java.util.Map;

@Category(MetastoreCheckinTest.class)
public class TestRemoteHiveMetaStoreCustomAuth extends TestRemoteHiveMetaStore {
  private static String correctUser = "correct_user";
  private static String correctPassword = "correct_passwd";
  private static String wrongPassword = "wrong_password";
  private static String wrongUser = "wrong_user";

  @Before
  public void setUp() throws Exception {
    initConf();
    MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION, "CUSTOM");
    MetastoreConf.setVar(conf, ConfVars.THRIFT_CUSTOM_AUTHENTICATION_CLASS,
            "org.apache.hadoop.hive.metastore.TestRemoteHiveMetaStoreCustomAuth$SimpleAuthenticationProviderImpl");
    MetastoreConf.setBoolVar(conf, ConfVars.EXECUTE_SET_UGI, false);
    super.setUp();
  }

  @Override
  protected HiveMetaStoreClient createClient() throws Exception {
    boolean gotException = false;
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setBoolVar(conf, ConfVars.USE_THRIFT_PASSWORD_AUTH, true);

    try {
      MetastoreConf.setVar(conf, ConfVars.THRIFT_AUTH_USERNAME, wrongUser);
      MetastoreConf.setVar(conf, ConfVars.THRIFT_AUTH_PASSWORD, wrongPassword);
      HiveMetaStoreClient tmpClient = new HiveMetaStoreClient(conf);
    } catch (Exception e) {
      gotException = true;
    }
    // Trying to log in using wrong username and password should fail
    Assert.assertTrue(gotException);

    MetastoreConf.setVar(conf, ConfVars.THRIFT_AUTH_USERNAME, correctUser);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_AUTH_PASSWORD, correctPassword);
    return new HiveMetaStoreClient(conf);
  }

  public static class SimpleAuthenticationProviderImpl implements MetaStorePasswdAuthenticationProvider {

    private Map<String, String> userMap = new HashMap<>();

    public SimpleAuthenticationProviderImpl() {
      init();
    }

    private void init(){
      userMap.put(correctUser, correctPassword);
    }

    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {

      if(!userMap.containsKey(user)) {
        throw new AuthenticationException("Invalid user : "+user);
      }
      if(!userMap.get(user).equals(password)){
        throw new AuthenticationException("Invalid passwd : "+password);
      }
    }
  }
}
