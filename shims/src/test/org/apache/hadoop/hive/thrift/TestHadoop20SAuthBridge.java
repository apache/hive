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

package org.apache.hadoop.hive.thrift;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.PrivilegedExceptionAction;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

public class TestHadoop20SAuthBridge extends TestCase {

  private static class MyHadoopThriftAuthBridge20S extends HadoopThriftAuthBridge20S {
    @Override
    public Server createServer(String keytabFile, String principalConf)
    throws TTransportException {
      //Create a Server that doesn't interpret any Kerberos stuff
      return new Server();
    }

    static class Server extends HadoopThriftAuthBridge20S.Server {
      public Server() throws TTransportException {
        super();
      }
      @Override
      public TTransportFactory createTransportFactory()
      throws TTransportException {
        TSaslServerTransport.Factory transFactory =
          new TSaslServerTransport.Factory();
        transFactory.addServerDefinition(AuthMethod.DIGEST.getMechanismName(),
            null, SaslRpcServer.SASL_DEFAULT_REALM,
            SaslRpcServer.SASL_PROPS,
            new SaslDigestCallbackHandler(secretManager));

        return new TUGIAssumingTransportFactory(transFactory, realUgi);
      }
    }
  }
  private static final int port = 10000;

  private final HiveConf conf;

  public TestHadoop20SAuthBridge(String name) {
    super(name);
    System.setProperty(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname,
        "true");
    System.setProperty(HiveConf.ConfVars.METASTOREURIS.varname,
        "thrift://localhost:" + port);
    System.setProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, new Path(
        System.getProperty("test.build.data", "/tmp")).toString());
    conf = new HiveConf(TestHadoop20SAuthBridge.class);
    conf.setBoolean("hive.metastore.local", false);
  }

  public void testSaslWithHiveMetaStore() throws Exception {

    Thread thread = new Thread(new Runnable() {
      public void run() {
        try {
          HiveMetaStore.startMetaStore(port,new MyHadoopThriftAuthBridge20S());
        } catch (Throwable e) {
          System.exit(1);
        }
      }
    });
    thread.setDaemon(true);
    thread.start();
    loopUntilHMSReady();
    UserGroupInformation clientUgi = UserGroupInformation.getCurrentUser();
    obtainTokenAndAddIntoUGI(clientUgi, null);
    obtainTokenAndAddIntoUGI(clientUgi, "tokenForFooTablePartition");
  }

  private void obtainTokenAndAddIntoUGI(UserGroupInformation clientUgi,
      String tokenSig) throws Exception {
    //obtain a token by directly invoking the metastore operation(without going
    //through the thrift interface). Obtaining a token makes the secret manager
    //aware of the user and that it gave the token to the user
    String tokenStrForm;
    if (tokenSig == null) {
      tokenStrForm =
        HiveMetaStore.getDelegationToken(clientUgi.getShortUserName());
    } else {
      tokenStrForm =
        HiveMetaStore.getDelegationToken(clientUgi.getShortUserName(),
                                         tokenSig);
      conf.set("hive.metastore.token.signature", tokenSig);
    }

    Token<DelegationTokenIdentifier> t= new Token<DelegationTokenIdentifier>();
    t.decodeFromUrlString(tokenStrForm);
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

    assertTrue("Couldn't connect to metastore", hiveClient != null);

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
            HiveMetaStoreClient hiveClient =
              new HiveMetaStoreClient(conf);
            return hiveClient;
          } catch (MetaException e) {
            return null;
          }
        }
      });
    assertTrue("Expected metastore operations to fail", hiveClient == null);
  }

  /**
   * A simple connect test to make sure that the metastore is up
   * @throws Exception
   */
  private void loopUntilHMSReady() throws Exception {
    int retries = 0;
    Exception exc = null;
    while (true) {
      try {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(port), 5000);
        socket.close();
        return;
      } catch (Exception e) {
        if (retries++ > 6) { //give up
          exc = e;
          break;
        }
        Thread.sleep(10000);
      }
    }
    throw exc;
  }

  private void createDBAndVerifyExistence(HiveMetaStoreClient client)
  throws Exception {
    String dbName = "simpdb";
    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);
    Database db1 = client.getDatabase(dbName);
    client.dropDatabase(dbName);
    assertTrue("Databases do not match", db1.getName().equals(db.getName()));
  }
}
