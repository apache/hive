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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.DefaultImpersonationProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

public class TestHadoopAuthBridge23 {

  /**
   * set to true when metastore token manager has intitialized token manager
   * through call to HadoopThriftAuthBridge23.Server.startDelegationTokenSecretManager
   */
  static volatile boolean isMetastoreTokenManagerInited;

  public static class MyTokenStore extends MemoryTokenStore {
    static volatile DelegationTokenStore TOKEN_STORE = null;
    public void init(Object hmsHandler, HadoopThriftAuthBridge.Server.ServerMode smode) throws TokenStoreException {
      super.init(hmsHandler, smode);
      TOKEN_STORE = this;
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      isMetastoreTokenManagerInited = true;
    }
  }

  private static class MyHadoopThriftAuthBridge23 extends HadoopThriftAuthBridge23 {
    @Override
    public Server createServer(String keytabFile, String principalConf, String clientConf)
    throws TTransportException {
      //Create a Server that doesn't interpret any Kerberos stuff
      return new Server();
    }

    static class Server extends HadoopThriftAuthBridge.Server {
      public Server() throws TTransportException {
        super();
      }
      @Override
      public TTransportFactory createTransportFactory(Map<String, String> saslProps)
      throws TTransportException {
        TSaslServerTransport.Factory transFactory =
          new TSaslServerTransport.Factory();
        transFactory.addServerDefinition(AuthMethod.DIGEST.getMechanismName(),
            null, SaslRpcServer.SASL_DEFAULT_REALM,
            saslProps,
            new SaslDigestCallbackHandler(secretManager));

        return new TUGIAssumingTransportFactory(transFactory, realUgi);
      }


    }
  }


  private HiveConf conf;

  private void configureSuperUserIPAddresses(Configuration conf,
      String superUserShortName) throws IOException {
    List<String> ipList = new ArrayList<String>();
    Enumeration<NetworkInterface> netInterfaceList = NetworkInterface
        .getNetworkInterfaces();
    while (netInterfaceList.hasMoreElements()) {
      NetworkInterface inf = netInterfaceList.nextElement();
      Enumeration<InetAddress> addrList = inf.getInetAddresses();
      while (addrList.hasMoreElements()) {
        InetAddress addr = addrList.nextElement();
        ipList.add(addr.getHostAddress());
      }
    }
    StringBuilder builder = new StringBuilder();
    for (String ip : ipList) {
      builder.append(ip);
      builder.append(',');
    }
    builder.append("127.0.1.1,");
    builder.append(InetAddress.getLocalHost().getCanonicalHostName());
    conf.setStrings(DefaultImpersonationProvider.getTestProvider().getProxySuperuserIpConfKey(superUserShortName),
        builder.toString());
  }
  @Before
  public void setup() throws Exception {
    isMetastoreTokenManagerInited = false;
    int port = findFreePort();
    System.setProperty(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname,
        "true");
    System.setProperty(HiveConf.ConfVars.METASTOREURIS.varname,
        "thrift://localhost:" + port);
    System.setProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, new Path(
        System.getProperty("test.build.data", "/tmp")).toString());
    System.setProperty(HiveConf.ConfVars.METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_CLS.varname,
        MyTokenStore.class.getName());
    conf = new HiveConf(TestHadoopAuthBridge23.class);
    MetaStoreTestUtils.startMetaStore(port, new MyHadoopThriftAuthBridge23());
  }

  /**
   * Test delegation token store/load from shared store.
   * @throws Exception
   */
  @Test
  public void testDelegationTokenSharedStore() throws Exception {
    UserGroupInformation clientUgi = UserGroupInformation.getCurrentUser();

    TokenStoreDelegationTokenSecretManager tokenManager =
        new TokenStoreDelegationTokenSecretManager(0, 60*60*1000, 60*60*1000, 0,
            MyTokenStore.TOKEN_STORE);
    // initializes current key
    tokenManager.startThreads();
    tokenManager.stopThreads();

    String tokenStrForm =
        tokenManager.getDelegationToken(clientUgi.getShortUserName(), clientUgi.getShortUserName());
    Token<DelegationTokenIdentifier> t= new Token<DelegationTokenIdentifier>();
    t.decodeFromUrlString(tokenStrForm);

    //check whether the username in the token is what we expect
    DelegationTokenIdentifier d = new DelegationTokenIdentifier();
    d.readFields(new DataInputStream(new ByteArrayInputStream(
        t.getIdentifier())));
    Assert.assertTrue("Usernames don't match",
        clientUgi.getShortUserName().equals(d.getUser().getShortUserName()));

    DelegationTokenInformation tokenInfo = MyTokenStore.TOKEN_STORE
        .getToken(d);
    Assert.assertNotNull("token not in store", tokenInfo);
    Assert.assertFalse("duplicate token add",
        MyTokenStore.TOKEN_STORE.addToken(d, tokenInfo));

    // check keys are copied from token store when token is loaded
    TokenStoreDelegationTokenSecretManager anotherManager =
        new TokenStoreDelegationTokenSecretManager(0, 0, 0, 0,
            MyTokenStore.TOKEN_STORE);
    Assert.assertEquals("master keys empty on init", 0,
        anotherManager.getAllKeys().length);
    Assert.assertNotNull("token loaded",
        anotherManager.retrievePassword(d));
    anotherManager.renewToken(t, clientUgi.getShortUserName());
    Assert.assertEquals("master keys not loaded from store",
        MyTokenStore.TOKEN_STORE.getMasterKeys().length,
        anotherManager.getAllKeys().length);

    // cancel the delegation token
    tokenManager.cancelDelegationToken(tokenStrForm);
    Assert.assertNull("token not removed from store after cancel",
        MyTokenStore.TOKEN_STORE.getToken(d));
    Assert.assertFalse("token removed (again)",
        MyTokenStore.TOKEN_STORE.removeToken(d));
    try {
      anotherManager.retrievePassword(d);
      Assert.fail("InvalidToken expected after cancel");
    } catch (InvalidToken ex) {
      // expected
    }

    // token expiration
    MyTokenStore.TOKEN_STORE.addToken(d,
        new DelegationTokenInformation(0, t.getPassword()));
    Assert.assertNotNull(MyTokenStore.TOKEN_STORE.getToken(d));
    anotherManager.removeExpiredTokens();
    Assert.assertNull("Expired token not removed",
        MyTokenStore.TOKEN_STORE.getToken(d));

    // key expiration - create an already expired key
    anotherManager.startThreads(); // generates initial key
    anotherManager.stopThreads();
    DelegationKey expiredKey = new DelegationKey(-1, 0, anotherManager.getAllKeys()[0].getKey());
    anotherManager.logUpdateMasterKey(expiredKey); // updates key with sequence number
    Assert.assertTrue("expired key not in allKeys",
        anotherManager.reloadKeys().containsKey(expiredKey.getKeyId()));
    anotherManager.rollMasterKeyExt();
    Assert.assertFalse("Expired key not removed",
        anotherManager.reloadKeys().containsKey(expiredKey.getKeyId()));
  }

  @Test
  public void testSaslWithHiveMetaStore() throws Exception {
    setup();
    UserGroupInformation clientUgi = UserGroupInformation.getCurrentUser();
    obtainTokenAndAddIntoUGI(clientUgi, null);
    obtainTokenAndAddIntoUGI(clientUgi, "tokenForFooTablePartition");
  }

  @Test
  public void testMetastoreProxyUser() throws Exception {
    setup();

    final String proxyUserName = "proxyUser";
    //set the configuration up such that proxyUser can act on
    //behalf of all users belonging to the group foo_bar_group (
    //a dummy group)
    String[] groupNames =
      new String[] { "foo_bar_group" };
    setGroupsInConf(groupNames, proxyUserName);

    final UserGroupInformation delegationTokenUser =
      UserGroupInformation.getCurrentUser();

    final UserGroupInformation proxyUserUgi =
      UserGroupInformation.createRemoteUser(proxyUserName);
    String tokenStrForm = proxyUserUgi.doAs(new PrivilegedExceptionAction<String>() {
      public String run() throws Exception {
        try {
          //Since the user running the test won't belong to a non-existent group
          //foo_bar_group, the call to getDelegationTokenStr will fail
          return getDelegationTokenStr(delegationTokenUser, proxyUserUgi);
        } catch (AuthorizationException ae) {
          return null;
        }
      }
    });
    Assert.assertTrue("Expected the getDelegationToken call to fail",
        tokenStrForm == null);

    //set the configuration up such that proxyUser can act on
    //behalf of all users belonging to the real group(s) that the
    //user running the test belongs to
    setGroupsInConf(UserGroupInformation.getCurrentUser().getGroupNames(),
        proxyUserName);
    tokenStrForm = proxyUserUgi.doAs(new PrivilegedExceptionAction<String>() {
      public String run() throws Exception {
        try {
          //Since the user running the test belongs to the group
          //obtained above the call to getDelegationTokenStr will succeed
          return getDelegationTokenStr(delegationTokenUser, proxyUserUgi);
        } catch (AuthorizationException ae) {
          return null;
        }
      }
    });
    Assert.assertTrue("Expected the getDelegationToken call to not fail",
        tokenStrForm != null);
    Token<DelegationTokenIdentifier> t= new Token<DelegationTokenIdentifier>();
    t.decodeFromUrlString(tokenStrForm);
    //check whether the username in the token is what we expect
    DelegationTokenIdentifier d = new DelegationTokenIdentifier();
    d.readFields(new DataInputStream(new ByteArrayInputStream(
        t.getIdentifier())));
    Assert.assertTrue("Usernames don't match",
        delegationTokenUser.getShortUserName().equals(d.getUser().getShortUserName()));

  }

  private void setGroupsInConf(String[] groupNames, String proxyUserName)
  throws IOException {
   conf.set(
      DefaultImpersonationProvider.getTestProvider().getProxySuperuserGroupConfKey(proxyUserName),
      StringUtils.join(",", Arrays.asList(groupNames)));
    configureSuperUserIPAddresses(conf, proxyUserName);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }

  private String getDelegationTokenStr(UserGroupInformation ownerUgi,
      UserGroupInformation realUgi) throws Exception {
    //obtain a token by directly invoking the metastore operation(without going
    //through the thrift interface). Obtaining a token makes the secret manager
    //aware of the user and that it gave the token to the user
    //also set the authentication method explicitly to KERBEROS. Since the
    //metastore checks whether the authentication method is KERBEROS or not
    //for getDelegationToken, and the testcases don't use
    //kerberos, this needs to be done

    waitForMetastoreTokenInit();

    HadoopThriftAuthBridge.Server.authenticationMethod
                             .set(AuthenticationMethod.KERBEROS);
    return
        HiveMetaStore.getDelegationToken(ownerUgi.getShortUserName(),
            realUgi.getShortUserName(), InetAddress.getLocalHost().getHostAddress());
  }

  /**
   * Wait for metastore to have initialized token manager
   * This does not have to be done in other metastore test cases as they
   * use metastore client which will retry few times on failure
   * @throws InterruptedException
   */
  private void waitForMetastoreTokenInit() throws InterruptedException {
    int waitAttempts = 30;
    while(waitAttempts > 0 && !isMetastoreTokenManagerInited){
      Thread.sleep(1000);
      waitAttempts--;
    }
  }

  private void obtainTokenAndAddIntoUGI(UserGroupInformation clientUgi,
      String tokenSig) throws Exception {
    String tokenStrForm = getDelegationTokenStr(clientUgi, clientUgi);
    Token<DelegationTokenIdentifier> t= new Token<DelegationTokenIdentifier>();
    t.decodeFromUrlString(tokenStrForm);

    //check whether the username in the token is what we expect
    DelegationTokenIdentifier d = new DelegationTokenIdentifier();
    d.readFields(new DataInputStream(new ByteArrayInputStream(
        t.getIdentifier())));
    Assert.assertTrue("Usernames don't match",
        clientUgi.getShortUserName().equals(d.getUser().getShortUserName()));

    if (tokenSig != null) {
      conf.setVar(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE, tokenSig);
      t.setService(new Text(tokenSig));
    }
    //add the token to the clientUgi for securely talking to the metastore
    clientUgi.addToken(t);
    //Create the metastore client as the clientUgi. Doing so this
    //way will give the client access to the token that was added earlier
    //in the clientUgi
    HiveMetaStoreClient hiveClient =
      clientUgi.doAs(new PrivilegedExceptionAction<HiveMetaStoreClient>() {
        public HiveMetaStoreClient run() throws Exception {
          HiveMetaStoreClient hiveClient =
            new HiveMetaStoreClient(conf);
          return hiveClient;
        }
      });

    Assert.assertTrue("Couldn't connect to metastore", hiveClient != null);

    //try out some metastore operations
    createDBAndVerifyExistence(hiveClient);

    hiveClient.close();

    //Now cancel the delegation token
    HiveMetaStore.cancelDelegationToken(tokenStrForm);

    //now metastore connection should fail
    hiveClient =
      clientUgi.doAs(new PrivilegedExceptionAction<HiveMetaStoreClient>() {
        public HiveMetaStoreClient run() {
          try {
            return new HiveMetaStoreClient(conf);
          } catch (MetaException e) {
            return null;
          }
        }
      });
    Assert.assertTrue("Expected metastore operations to fail", hiveClient == null);
  }

  private void createDBAndVerifyExistence(HiveMetaStoreClient client)
  throws Exception {
    String dbName = "simpdb";
    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);
    Database db1 = client.getDatabase(dbName);
    client.dropDatabase(dbName);
    Assert.assertTrue("Databases do not match", db1.getName().equals(db.getName()));
  }

  private int findFreePort() throws IOException {
    ServerSocket socket= new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }
}
