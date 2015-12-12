/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.security;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;

public class SecretManager extends ZKDelegationTokenSecretManager<LlapTokenIdentifier> {
  public SecretManager(Configuration conf) {
    super(conf);
  }

  @Override
  public LlapTokenIdentifier createIdentifier() {
    return new LlapTokenIdentifier();
  }

  @Override
  public LlapTokenIdentifier decodeTokenIdentifier(
      Token<LlapTokenIdentifier> token) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(token.getIdentifier()));
    LlapTokenIdentifier id = new LlapTokenIdentifier();
    id.readFields(dis);
    dis.close();
    return id;
  }

  public static SecretManager createSecretManager(
      final Configuration conf, String llapPrincipal, String llapKeytab) {
    // Create ZK connection under a separate ugi (if specified) - ZK works in mysterious ways.
    UserGroupInformation zkUgi = null;
    String principal = HiveConf.getVar(conf, ConfVars.LLAP_ZKSM_KERBEROS_PRINCIPAL, llapPrincipal);
    String keyTab = HiveConf.getVar(conf, ConfVars.LLAP_ZKSM_KERBEROS_KEYTAB_FILE, llapKeytab);
    try {
      zkUgi = LlapSecurityHelper.loginWithKerberos(principal, keyTab);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // Override the default delegation token lifetime for LLAP.
    // Also set all the necessary ZK settings to defaults and LLAP configs, if not set.
    final Configuration zkConf = new Configuration(conf);
    zkConf.setLong(DelegationTokenManager.MAX_LIFETIME,
        HiveConf.getTimeVar(conf, ConfVars.LLAP_DELEGATION_TOKEN_LIFETIME, TimeUnit.SECONDS));
    zkConf.set(SecretManager.ZK_DTSM_ZK_KERBEROS_PRINCIPAL, principal);
    zkConf.set(SecretManager.ZK_DTSM_ZK_KERBEROS_KEYTAB, keyTab);
    setZkConfIfNotSet(zkConf, SecretManager.ZK_DTSM_ZNODE_WORKING_PATH, "llapzkdtsm");
    setZkConfIfNotSet(zkConf, SecretManager.ZK_DTSM_ZK_AUTH_TYPE, "sasl");
    setZkConfIfNotSet(zkConf, SecretManager.ZK_DTSM_ZK_CONNECTION_STRING,
        HiveConf.getVar(zkConf, ConfVars.LLAP_ZKSM_ZK_CONNECTION_STRING));
    return zkUgi.doAs(new PrivilegedAction<SecretManager>() {
      @Override
      public SecretManager run() {
        SecretManager zkSecretManager = new SecretManager(zkConf);
        try {
          zkSecretManager.startThreads();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return zkSecretManager;
      }
    });
  }

  private static void setZkConfIfNotSet(Configuration zkConf, String name, String value) {
    if (zkConf.get(name) != null) return;
    zkConf.set(name, value);
  }
}