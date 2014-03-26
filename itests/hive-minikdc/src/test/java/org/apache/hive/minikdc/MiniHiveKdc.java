/**
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

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;

import com.google.common.io.Files;

public class MiniHiveKdc {
  public static String HIVE_SERVICE_PRINCIPAL = "hive";
  public static String HIVE_TEST_USER_1 = "user1";
  public static String HIVE_TEST_USER_2 = "user2";
  public static String HIVE_TEST_SUPER_USER = "superuser";

  private MiniKdc miniKdc;
  private File workDir;
  private Configuration conf;
  private Map<String, String> userPrincipals =
      new HashMap<String, String>();
  private Properties kdcConf = MiniKdc.createConf();
  private int keyTabCounter = 1;

  // hadoop group mapping that maps user to same group
  public static class HiveTestSimpleGroupMapping implements GroupMappingServiceProvider {
    @Override
    public List<String> getGroups(String user) throws IOException {
      List<String> results = new ArrayList<String>();
      results.add(user);
      return results;
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
    }
  }

  public static MiniHiveKdc getMiniHiveKdc (Configuration conf) throws Exception {
    return new MiniHiveKdc(conf);
  }

  public MiniHiveKdc(Configuration conf)
      throws Exception {
    File baseDir =  Files.createTempDir();
    baseDir.deleteOnExit();
    workDir = new File (baseDir, "HiveMiniKdc");
    this.conf = conf;

    /**
     *  Hadoop security classes read the default realm via static initialization,
     *  before miniKdc is initialized. Hence we set the realm via a test configuration
     *  and propagate that to miniKdc.
     */
    assertNotNull("java.security.krb5.conf is needed for hadoop security",
        System.getProperty("java.security.krb5.conf"));
    System.clearProperty("java.security.krb5.conf");

    miniKdc = new MiniKdc(kdcConf, new File(workDir, "miniKdc"));
    miniKdc.start();

    // create default users
    addUserPrincipal(getServicePrincipalForUser(HIVE_SERVICE_PRINCIPAL));
    addUserPrincipal(HIVE_TEST_USER_1);
    addUserPrincipal(HIVE_TEST_USER_2);
    addUserPrincipal(HIVE_TEST_SUPER_USER);
  }

  public String getKeyTabFile(String principalName) {
    return userPrincipals.get(principalName);
  }

  public void shutDown() {
    miniKdc.stop();
  }

  public void addUserPrincipal(String principal) throws Exception {
    File keytab = new File(workDir, "miniKdc" + keyTabCounter++ + ".keytab");
    miniKdc.createPrincipal(keytab, principal);
    userPrincipals.put(principal, keytab.getPath());
  }

  public UserGroupInformation loginUser(String principal)
      throws Exception {
    ShimLoader.getHadoopShims().loginUserFromKeytab(principal,
        getKeyTabFile(principal));
    return ShimLoader.getHadoopShims().getUGIForConf(conf);
  }

  public Properties getKdcConf() {
    return kdcConf;
  }

  public String getFullyQualifiedUserPrincipal(String shortUserName) {
    return shortUserName + "@" + miniKdc.getRealm();
  }

  public String getFullyQualifiedServicePrincipal(String shortUserName) {
    return getServicePrincipalForUser(shortUserName) + "@" + miniKdc.getRealm();
  }

  public String getServicePrincipalForUser(String shortUserName) {
    return shortUserName + "/" + miniKdc.getHost();
  }

  public String getHiveServicePrincipal() {
    return getServicePrincipalForUser(HIVE_SERVICE_PRINCIPAL);
  }

  public String getFullHiveServicePrincipal() {
    return getServicePrincipalForUser(HIVE_SERVICE_PRINCIPAL) + "@" + miniKdc.getRealm();
  }

  public String getDefaultUserPrincipal() {
    return HIVE_TEST_USER_1;
  }


}
