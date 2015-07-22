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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Enumeration;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHiveMetaStoreClientWithMiniKdc {
  private MiniHiveKdc miniHiveKdc;
  private HiveConf hiveConf;
  private ServerSocket serverSock;
  private int port;
  private HiveMetaStoreClient msc;
  private String mscIP;

  @Before
  public void setUp() throws Exception {
    hiveConf = new HiveConf();
    port = MetaStoreUtils.findFreePort();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);

    miniHiveKdc = MiniHiveKdc.getMiniHiveKdc(hiveConf);
    mscIP = getInterface();
    // add service principal in miniHiveKdc
    miniHiveKdc.addServicePrincipalWithHost(mscIP);
  }

  @After
  public void tearDown() throws Exception {
    miniHiveKdc.shutDown();
  }

  @Test
  public void testHiveMetaStoreClientConnection() throws Exception {
 Socket clientSock = null;
    try {
      serverSock = new ServerSocket(port);

      setUGI();

      /* HiveMetastoreClient */
      HMClient client = new HMClient();
      client.start();

      clientSock = serverSock.accept();
      assertEquals(mscIP, clientSock.getInetAddress().getHostAddress());

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      serverSock.close();
    }
  }

  private void setUGI() throws Exception {
    String servicePrinc = miniHiveKdc.getHiveServicePrincipal(mscIP);
    miniHiveKdc.loginUser(servicePrinc);
  }

  private String getInterface() throws Exception {
    Enumeration nics;
    nics = NetworkInterface.getNetworkInterfaces();

    while(nics.hasMoreElements())
    {
      NetworkInterface nic = (NetworkInterface) nics.nextElement();
      Enumeration nicaddrs = nic.getInetAddresses();
      while (nicaddrs.hasMoreElements())
      {
        InetAddress addr = (InetAddress) nicaddrs.nextElement();
        System.out.println(addr.getHostAddress());
        // only check IPv4
        if (addr.getHostAddress().contains(".")
            && !addr.getHostAddress().equals("127.0.0.1")) {
          return addr.getHostAddress();
        }
      }
    }

    /* If the system does not have other network address,
     * it returns default localhost */
    return "127.0.0.1";
  }

  /**
   *  HiveMetaStore client: It will open the socket connection when we create new object.
   */
  private class HMClient extends Thread {
    public HMClient() {
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    }

    @Override
    public void run() {
      try {
        Thread.sleep(1000);
        msc = new HiveMetaStoreClient(hiveConf, null);
      } catch (Throwable e) {
        System.err.println("Unable to open the metastore");
        System.err.println(StringUtils.stringifyException(e));
      }
    }
  }
}
