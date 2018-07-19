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


package org.apache.hadoop.hive.metastore.security;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.ReflectionUtils;

public class MetastoreDelegationTokenManager {
  public static final String DELEGATION_TOKEN_STORE_ZK_CONNECT_STR =
      "hive.cluster.delegation.token.store.zookeeper.connectString";
  protected DelegationTokenSecretManager secretManager;
  // Alternate connect string specification configuration
  public static final String DELEGATION_TOKEN_STORE_ZK_CONNECT_STR_ALTERNATE =
      "hive.zookeeper.quorum";

  public static final String DELEGATION_TOKEN_STORE_ZK_CONNECT_TIMEOUTMILLIS =
      "hive.cluster.delegation.token.store.zookeeper.connectTimeoutMillis";
  public static final String DELEGATION_TOKEN_STORE_ZK_ZNODE =
      "hive.cluster.delegation.token.store.zookeeper.znode";
  public static final String DELEGATION_TOKEN_STORE_ZK_ACL =
      "hive.cluster.delegation.token.store.zookeeper.acl";
  public static final String DELEGATION_TOKEN_STORE_ZK_ZNODE_DEFAULT = "/hivedelegation";

  public MetastoreDelegationTokenManager() {
  }

  public DelegationTokenSecretManager getSecretManager() {
    return secretManager;
  }

  public void startDelegationTokenSecretManager(Configuration conf, Object hms) throws IOException {
    startDelegationTokenSecretManager(conf, hms, HadoopThriftAuthBridge.Server.ServerMode.METASTORE);
  }

  public void startDelegationTokenSecretManager(Configuration conf, Object hms, HadoopThriftAuthBridge.Server.ServerMode smode)
      throws IOException {
    long secretKeyInterval = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.DELEGATION_KEY_UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
    long tokenMaxLifetime = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.DELEGATION_TOKEN_MAX_LIFETIME, TimeUnit.MILLISECONDS);
    long tokenRenewInterval = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.DELEGATION_TOKEN_RENEW_INTERVAL, TimeUnit.MILLISECONDS);
    long tokenGcInterval = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.DELEGATION_TOKEN_GC_INTERVAL, TimeUnit.MILLISECONDS);

    DelegationTokenStore dts = getTokenStore(conf);
    dts.setConf(conf);
    dts.init(hms, smode);
    secretManager =
        new TokenStoreDelegationTokenSecretManager(secretKeyInterval, tokenMaxLifetime,
            tokenRenewInterval, tokenGcInterval, dts);
    secretManager.startThreads();
  }

  public String getDelegationToken(final String owner, final String renewer, String remoteAddr)
      throws IOException,
      InterruptedException {
    /*
     * If the user asking the token is same as the 'owner' then don't do
     * any proxy authorization checks. For cases like oozie, where it gets
     * a delegation token for another user, we need to make sure oozie is
     * authorized to get a delegation token.
     */
    // Do all checks on short names
    UserGroupInformation currUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation ownerUgi = UserGroupInformation.createRemoteUser(owner);
    if (!ownerUgi.getShortUserName().equals(currUser.getShortUserName())) {
      // in the case of proxy users, the getCurrentUser will return the
      // real user (for e.g. oozie) due to the doAs that happened just before the
      // server started executing the method getDelegationToken in the MetaStore
      ownerUgi = UserGroupInformation.createProxyUser(owner, UserGroupInformation.getCurrentUser());
      ProxyUsers.authorize(ownerUgi, remoteAddr, null);
    }
    //if impersonation is turned on this called using the HiveSessionImplWithUGI
    //using sessionProxy. so the currentUser will be the impersonated user here eg. oozie
    //we cannot create a proxy user which represents Oozie's client user here since
    //we cannot authenticate it using Kerberos/Digest. We trust the user which opened
    //session using Kerberos in this case.
    //if impersonation is turned off, the current user is Hive which can open
    //kerberos connections to HMS if required.
    return secretManager.getDelegationToken(owner, renewer);
  }

  public String getDelegationTokenWithService(String owner, String renewer, String service, String remoteAddr)
      throws IOException, InterruptedException {
    String token = getDelegationToken(owner, renewer, remoteAddr);
    return addServiceToToken(token, service);
  }

  public long renewDelegationToken(String tokenStrForm)
      throws IOException {
    return secretManager.renewDelegationToken(tokenStrForm);
  }

  public String getUserFromToken(String tokenStr) throws IOException {
    return secretManager.getUserFromToken(tokenStr);
  }

  public void cancelDelegationToken(String tokenStrForm) throws IOException {
    secretManager.cancelDelegationToken(tokenStrForm);
  }

  /**
   * Verify token string
   * @param tokenStrForm
   * @return user name
   * @throws IOException
   */
  public String verifyDelegationToken(String tokenStrForm) throws IOException {
    return secretManager.verifyDelegationToken(tokenStrForm);
  }

  private DelegationTokenStore getTokenStore(Configuration conf) throws IOException {
    String tokenStoreClassName = SecurityUtils.getTokenStoreClassName(conf);
    try {
      Class<? extends DelegationTokenStore> storeClass =
          Class.forName(tokenStoreClassName).asSubclass(DelegationTokenStore.class);
      return ReflectionUtils.newInstance(storeClass, conf);
    } catch (ClassNotFoundException e) {
      throw new IOException("Error initializing delegation token store: " + tokenStoreClassName, e);
    }
  }

  /**
   * Add a given service to delegation token string.
   * @param tokenStr
   * @param tokenService
   * @return
   * @throws IOException
   */
  public static String addServiceToToken(String tokenStr, String tokenService)
      throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = createToken(tokenStr, tokenService);
    return delegationToken.encodeToUrlString();
  }

  /**
   * Create a new token using the given string and service
   * @param tokenStr
   * @param tokenService
   * @return
   * @throws IOException
   */
  private static Token<DelegationTokenIdentifier> createToken(String tokenStr, String tokenService)
      throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = new Token<>();
    delegationToken.decodeFromUrlString(tokenStr);
    delegationToken.setService(new Text(tokenService));
    return delegationToken;
  }

}
