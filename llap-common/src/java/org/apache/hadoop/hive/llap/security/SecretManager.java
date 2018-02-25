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
package org.apache.hadoop.hive.llap.security;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.security.token.delegation.HiveDelegationTokenSupport;
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecretManager extends ZKDelegationTokenSecretManager<LlapTokenIdentifier>
  implements SigningSecretManager {
  private static final Logger LOG = LoggerFactory.getLogger(SecretManager.class);
  private static final String DISABLE_MESSAGE =
      "Set " + ConfVars.LLAP_VALIDATE_ACLS.varname + " to false to disable ACL validation (note"
      +  " that invalid ACLs on secret key paths would mean that security is compromised)";
  private final Configuration conf;
  private final String clusterId;

  public SecretManager(Configuration conf, String clusterId) {
    super(validateConfigBeforeCtor(conf));
    this.clusterId = clusterId;
    this.conf = conf;
    checkForZKDTSMBug();
  }

  private static Configuration validateConfigBeforeCtor(Configuration conf) {
    setCurator(null); // Ensure there's no threadlocal. We don't expect one.
    // We don't ever want to create key paths with world visibility. Why is that even an option?!!
    String authType = conf.get(ZK_DTSM_ZK_AUTH_TYPE);
    if (!"sasl".equals(authType)) {
      throw new RuntimeException("Inconsistent configuration: secure cluster, but ZK auth is "
          + authType + " instead of sasl");
    }
    return conf;
  }

  @Override
  public void startThreads() throws IOException {
    String principalUser = LlapUtil.getUserNameFromPrincipal(
        conf.get(SecretManager.ZK_DTSM_ZK_KERBEROS_PRINCIPAL));
    LOG.info("Starting ZK threads as user " + UserGroupInformation.getCurrentUser()
        + "; kerberos principal is configured for user (short user name) " + principalUser);
    super.startThreads();
    if (!HiveConf.getBoolVar(conf, ConfVars.LLAP_VALIDATE_ACLS)
      || !UserGroupInformation.isSecurityEnabled()) return;
    String path = conf.get(ZK_DTSM_ZNODE_WORKING_PATH, null);
    if (path == null) throw new AssertionError("Path was not set in config");
    checkRootAcls(conf, path, principalUser);
  }

  // Workaround for HADOOP-12659 - remove when Hadoop 2.7.X is no longer supported.
  private void checkForZKDTSMBug() {
    // There's a bug in ZKDelegationTokenSecretManager ctor where seconds are not converted to ms.
    long expectedRenewTimeSec = conf.getLong(DelegationTokenManager.RENEW_INTERVAL, -1);
    LOG.info("Checking for tokenRenewInterval bug: " + expectedRenewTimeSec);
    if (expectedRenewTimeSec == -1) return; // The default works, no bug.
    java.lang.reflect.Field f = null;
    try {
     Class<?> c = org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.class;
     f = c.getDeclaredField("tokenRenewInterval");
     f.setAccessible(true);
    } catch (Throwable t) {
      // Maybe someone removed the field; probably ok to ignore.
      LOG.error("Failed to check for tokenRenewInterval bug, hoping for the best", t);
      return;
    }
    try {
      long realValue = f.getLong(this);
      long expectedValue = expectedRenewTimeSec * 1000;
      LOG.info("tokenRenewInterval is: " + realValue + " (expected " + expectedValue + ")");
      if (realValue == expectedRenewTimeSec) {
        // Bug - the field has to be in ms, not sec. Override only if set precisely to sec.
        f.setLong(this, expectedValue);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Failed to address tokenRenewInterval bug", ex);
    }
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

  @Override
  public synchronized DelegationKey getCurrentKey() throws IOException {
    DelegationKey currentKey = getDelegationKey(getCurrentKeyId());
    if (currentKey != null) return currentKey;
    // Try to roll the key if none is found.
    HiveDelegationTokenSupport.rollMasterKey(this);
    return getDelegationKey(getCurrentKeyId());
  }

  @Override
  public byte[] signWithKey(byte[] message, DelegationKey key) {
    return createPassword(message, key.getKey());
  }

  @Override
  public byte[] signWithKey(byte[] message, int keyId) throws SecurityException {
    DelegationKey key = getDelegationKey(keyId);
    if (key == null) {
      throw new SecurityException("The key ID " + keyId + " was not found");
    }
    return createPassword(message, key.getKey());
  }

  static final class LlapZkConf {
    public Configuration zkConf;
    public UserGroupInformation zkUgi;
    public LlapZkConf(Configuration zkConf, UserGroupInformation zkUgi) {
      this.zkConf = zkConf;
      this.zkUgi = zkUgi;
    }
  }

  private static LlapZkConf createLlapZkConf(
      Configuration conf, String llapPrincipal, String llapKeytab, String clusterId) {
     String principal = HiveConf.getVar(conf, ConfVars.LLAP_ZKSM_KERBEROS_PRINCIPAL, llapPrincipal);
     String keyTab = HiveConf.getVar(conf, ConfVars.LLAP_ZKSM_KERBEROS_KEYTAB_FILE, llapKeytab);
     // Override the default delegation token lifetime for LLAP.
     // Also set all the necessary ZK settings to defaults and LLAP configs, if not set.
     final Configuration zkConf = new Configuration(conf);
    long tokenLifetime = HiveConf.getTimeVar(
        conf, ConfVars.LLAP_DELEGATION_TOKEN_LIFETIME, TimeUnit.SECONDS);
    zkConf.setLong(DelegationTokenManager.MAX_LIFETIME, tokenLifetime);
    zkConf.setLong(DelegationTokenManager.RENEW_INTERVAL, tokenLifetime);
    try {
      zkConf.set(ZK_DTSM_ZK_KERBEROS_PRINCIPAL,
          SecurityUtil.getServerPrincipal(principal, "0.0.0.0"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    zkConf.set(ZK_DTSM_ZK_KERBEROS_KEYTAB, keyTab);
    String zkPath = "zkdtsm_" + clusterId;
    LOG.info("Using {} as ZK secret manager path", zkPath);
    zkConf.set(ZK_DTSM_ZNODE_WORKING_PATH, zkPath);
    // Hardcode SASL here. ZKDTSM only supports none or sasl and we never want none.
    zkConf.set(ZK_DTSM_ZK_AUTH_TYPE, "sasl");
    long sessionTimeoutMs = HiveConf.getTimeVar(
        zkConf, ConfVars.LLAP_ZKSM_ZK_SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
    long newRetryCount =
        (ZK_DTSM_ZK_NUM_RETRIES_DEFAULT * sessionTimeoutMs) / ZK_DTSM_ZK_SESSION_TIMEOUT_DEFAULT;
    long connTimeoutMs = Math.max(sessionTimeoutMs, ZK_DTSM_ZK_CONNECTION_TIMEOUT_DEFAULT);
    zkConf.set(ZK_DTSM_ZK_SESSION_TIMEOUT, Long.toString(sessionTimeoutMs));
    zkConf.set(ZK_DTSM_ZK_CONNECTION_TIMEOUT, Long.toString(connTimeoutMs));
    zkConf.set(ZK_DTSM_ZK_NUM_RETRIES, Long.toString(newRetryCount));
    setZkConfIfNotSet(zkConf, ZK_DTSM_ZK_CONNECTION_STRING,
        HiveConf.getVar(zkConf, ConfVars.LLAP_ZKSM_ZK_CONNECTION_STRING));

    UserGroupInformation zkUgi = null;
    try {
      zkUgi = LlapUtil.loginWithKerberos(principal, keyTab);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new LlapZkConf(zkConf, zkUgi);
  }

  public static SecretManager createSecretManager(Configuration conf, String clusterId) {
    String llapPrincipal = HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_PRINCIPAL),
        llapKeytab = HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_KEYTAB_FILE);
    return createSecretManager(conf, llapPrincipal, llapKeytab, clusterId);
  }

  public static SecretManager createSecretManager(
      Configuration conf, String llapPrincipal, String llapKeytab, final String clusterId) {
    assert UserGroupInformation.isSecurityEnabled();
    final LlapZkConf c = createLlapZkConf(conf, llapPrincipal, llapKeytab, clusterId);
    return c.zkUgi.doAs(new PrivilegedAction<SecretManager>() {
      @Override
      public SecretManager run() {
        SecretManager zkSecretManager = new SecretManager(c.zkConf, clusterId);
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

  public Token<LlapTokenIdentifier> createLlapToken(
      String appId, String user, boolean isSignatureRequired) throws IOException {
    Text realUser = null, renewer = null;
    if (user == null) {
      UserGroupInformation ugi  = UserGroupInformation.getCurrentUser();
      user = ugi.getUserName();
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }
      renewer = new Text(ugi.getShortUserName());
    } else {
      renewer = new Text(user);
    }
    LlapTokenIdentifier llapId = new LlapTokenIdentifier(
        new Text(user), renewer, realUser, clusterId, appId, isSignatureRequired);
    // TODO: note that the token is not renewable right now and will last for 2 weeks by default.
    Token<LlapTokenIdentifier> token = new Token<LlapTokenIdentifier>(llapId, this);
    if (LOG.isInfoEnabled()) {
      LOG.info("Created LLAP token {}", token);
    }
    return token;
  }

  @Override
  public void close() {
    stopThreads();
  }

  private static void checkRootAcls(Configuration conf, String path, String user) {
    int stime = conf.getInt(ZK_DTSM_ZK_SESSION_TIMEOUT, ZK_DTSM_ZK_SESSION_TIMEOUT_DEFAULT),
        ctime = conf.getInt(ZK_DTSM_ZK_CONNECTION_TIMEOUT, ZK_DTSM_ZK_CONNECTION_TIMEOUT_DEFAULT);
    CuratorFramework zkClient = CuratorFrameworkFactory.builder().namespace(null)
        .retryPolicy(new RetryOneTime(10)).sessionTimeoutMs(stime).connectionTimeoutMs(ctime)
        .ensembleProvider(new FixedEnsembleProvider(conf.get(ZK_DTSM_ZK_CONNECTION_STRING)))
        .build();
    // Hardcoded from a private field in ZKDelegationTokenSecretManager.
    // We need to check the path under what it sets for namespace, since the namespace is
    // created with world ACLs.
    String nsPath = "/" + path + "/ZKDTSMRoot";
    Id currentUser = new Id("sasl", user);
    try {
      zkClient.start();
      List<String> children = zkClient.getChildren().forPath(nsPath);
      for (String child : children) {
        String childPath = nsPath + "/" + child;
        checkAcls(zkClient, currentUser, childPath);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      zkClient.close();
    }
  }

  private static void checkAcls(CuratorFramework zkClient, Id user, String path) {
    List<ACL> acls = null;
    try {
      acls = zkClient.getACL().forPath(path);
    } catch (Exception ex) {
      throw new RuntimeException("Error during the ACL check. " + DISABLE_MESSAGE, ex);
    }
    if (acls == null || acls.isEmpty()) {
      // There's some access (to get ACLs), so assume it means free for all.
      throw new SecurityException("No ACLs on "  + path + ". " + DISABLE_MESSAGE);
    }
    for (ACL acl : acls) {
      if (!user.equals(acl.getId())) {
        throw new SecurityException("The ACL " + acl + " is unnacceptable for " + path
            + "; only " + user + " is allowed. " + DISABLE_MESSAGE);
      }
    }
  }

  /** Verifies the token available as serialized bytes. */
  public void verifyToken(byte[] tokenBytes) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) return;
    if (tokenBytes == null) throw new SecurityException("Token required for authentication");
    Token<LlapTokenIdentifier> token = new Token<>();
    token.readFields(new DataInputStream(new ByteArrayInputStream(tokenBytes)));
    verifyToken(token.decodeIdentifier(), token.getPassword());
  }
}
