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

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStorePasswdAuthenticationProvider;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.Assert;
import org.junit.Before;

import javax.security.sasl.AuthenticationException;
import java.util.HashMap;
import java.util.Map;

public class TestRemoteHiveMetaStoreDualAuthKerb extends RemoteHiveMetaStoreDualAuthTest {

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected HiveMetaStoreClient createClient() throws Exception {
    boolean gotException = false;
    MetastoreConf.setVar(clientConf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setBoolVar(clientConf, ConfVars.USE_THRIFT_SASL, true);

    // Trying to log in using wrong username and password should fail
    try {
      MetastoreConf.setVar(clientConf, ConfVars.KERBEROS_KEYTAB_FILE, wrongKeytab);
      MetastoreConf.setVar(clientConf, ConfVars.KERBEROS_PRINCIPAL, wrongPrincipal);
      HiveMetaStoreClient tmpClient = new HiveMetaStoreClient(clientConf);
    } catch (Exception e) {
      gotException = true;
    }
    Assert.assertTrue(gotException);

    MetastoreConf.setVar(clientConf, ConfVars.KERBEROS_PRINCIPAL, hiveMetastorePrincipal);
    MetastoreConf.setVar(clientConf, ConfVars.KERBEROS_KEYTAB_FILE, hiveMetastoreKeytab);
    return new HiveMetaStoreClient(clientConf);
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
    public void authenticate(String user, String password) throws AuthenticationException {

      if(!userMap.containsKey(user)) {
        throw new AuthenticationException("Invalid user : "+user);
      }
      if(!userMap.get(user).equals(password)){
        throw new AuthenticationException("Invalid passwd : "+password);
      }
    }
  }
}
