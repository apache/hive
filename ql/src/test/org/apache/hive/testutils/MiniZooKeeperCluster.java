/*
 *
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
package org.apache.hive.testutils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.Reader;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * TODO: Most of the code in this class is ripped from ZooKeeper tests. Instead
 * of redoing it, we should contribute updates to their code which let us more
 * easily access testing helper objects.
 *
 *XXX: copied from the only used class by qtestutil from hbase-tests
 */
public class MiniZooKeeperCluster {
  private static final Logger LOG = LoggerFactory.getLogger(MiniZooKeeperCluster.class);

  private static final int TICK_TIME = 2000;
  private static final int DEFAULT_CONNECTION_TIMEOUT = 30000;
  private static final String LOCALHOST_KEY_STORE_NAME = "keystore.jks";
  private static final String TRUST_STORE_NAME = "truststore.jks";
  private static final String KEY_STORE_TRUST_STORE_PASSWORD = "HiveJdbc";
  private int connectionTimeout;

  private boolean started;

  /** The default port. If zero, we use a random port. */
  private int defaultClientPort = 0;

  private List<ServerCnxnFactory> standaloneServerFactoryList;
  private List<ZooKeeperServer> zooKeeperServers;
  private List<Integer> clientPortList;

  private int activeZKServerIndex;
  private int tickTime = 0;

  private Configuration configuration;

  private boolean sslEnabled = false;

  public MiniZooKeeperCluster() {
    this(new Configuration());
  }
  public MiniZooKeeperCluster(boolean sslEnabled) {
    this(new Configuration(), sslEnabled);
  }
  public MiniZooKeeperCluster(Configuration configuration) {
    this(configuration, false);
  }

  public MiniZooKeeperCluster(Configuration configuration, boolean sslEnabled) {
    this.sslEnabled = sslEnabled;
    this.started = false;
    this.configuration = configuration;
    activeZKServerIndex = -1;
    zooKeeperServers = new ArrayList<>();
    clientPortList = new ArrayList<>();
    standaloneServerFactoryList = new ArrayList<>();
    connectionTimeout = configuration.getInt(HConstants.ZK_SESSION_TIMEOUT + ".localHBaseCluster", DEFAULT_CONNECTION_TIMEOUT);
  }

  /**
   * Add a client port to the list.
   *
   * @param clientPort the specified port
   */
  public void addClientPort(int clientPort) {
    clientPortList.add(clientPort);
  }

  /**
   * Get the list of client ports.
   * @return clientPortList the client port list
   */
  @VisibleForTesting
  public List<Integer> getClientPortList() {
    return clientPortList;
  }

  /**
   * Check whether the client port in a specific position of the client port list is valid.
   *
   * @param index the specified position
   */
  private boolean hasValidClientPortInList(int index) {
    return (clientPortList.size() > index && clientPortList.get(index) > 0);
  }

  public void setDefaultClientPort(int clientPort) {
    if (clientPort <= 0) {
      throw new IllegalArgumentException("Invalid default ZK client port: " + clientPort);
    }
    this.defaultClientPort = clientPort;
  }

  /**
   * Selects a ZK client port.
   *
   * @param seedPort the seed port to start with; -1 means first time.
   * @Returns a valid and unused client port
   */
  private int selectClientPort(int seedPort) {
    int i;
    int returnClientPort = seedPort + 1;
    if (returnClientPort == 0) {
      // If the new port is invalid, find one - starting with the default client port.
      // If the default client port is not specified, starting with a random port.
      // The random port is selected from the range between 49152 to 65535. These ports cannot be
      // registered with IANA and are intended for dynamic allocation (see http://bit.ly/dynports).
      if (defaultClientPort > 0) {
        returnClientPort = defaultClientPort;
      } else {
        returnClientPort = 0xc000 + new Random().nextInt(0x3f00);
      }
    }
    // Make sure that the port is unused.
    while (true) {
      for (i = 0; i < clientPortList.size(); i++) {
        if (returnClientPort == clientPortList.get(i)) {
          // Already used. Update the port and retry.
          returnClientPort++;
          break;
        }
      }
      if (i == clientPortList.size()) {
        break; // found a unused port, exit
      }
    }
    return returnClientPort;
  }

  public void setTickTime(int tickTime) {
    this.tickTime = tickTime;
  }

  public int getBackupZooKeeperServerNum() {
    return zooKeeperServers.size() - 1;
  }

  public int getZooKeeperServerNum() {
    return zooKeeperServers.size();
  }

  // / XXX: From o.a.zk.t.ClientBase
  private static void setupTestEnv(boolean sslEnabled) {
    // With ZooKeeper 3.5 we need to whitelist the 4 letter commands we use
    System.setProperty("zookeeper.4lw.commands.whitelist", "*");

    // during the tests we run with 100K prealloc in the logs.
    // on windows systems prealloc of 64M was seen to take ~15seconds
    // resulting in test failure (client timeout on first session).
    // set env and directly in order to handle static init/gc issues
    System.setProperty("zookeeper.preAllocSize", "100");
    if (sslEnabled) {
      System.setProperty("zookeeper.authProvider.x509", "org.apache.zookeeper.server.auth.X509AuthenticationProvider");
    }
    FileTxnLog.setPreallocSize(100 * 1024);
  }

  public int startup(File baseDir) throws IOException, InterruptedException {
    int numZooKeeperServers = clientPortList.size();
    if (numZooKeeperServers == 0) {
      numZooKeeperServers = 1; // need at least 1 ZK server for testing
    }
    return startup(baseDir, numZooKeeperServers);
  }

  /**
   * @param baseDir
   * @param numZooKeeperServers
   * @return ClientPort server bound to, -1 if there was a
   *         binding problem and we couldn't pick another port.
   * @throws IOException
   * @throws InterruptedException
   */
  public int startup(File baseDir, int numZooKeeperServers) throws IOException, InterruptedException {
    if (numZooKeeperServers <= 0) {
      return -1;
    }

    setupTestEnv(sslEnabled);
    shutdown();

    int tentativePort = -1; // the seed port
    int currentClientPort;

    // running all the ZK servers
    for (int i = 0; i < numZooKeeperServers; i++) {
      File dir = new File(baseDir, "zookeeper_" + i).getAbsoluteFile();
      createDir(dir);
      int tickTimeToUse;
      if (this.tickTime > 0) {
        tickTimeToUse = this.tickTime;
      } else {
        tickTimeToUse = TICK_TIME;
      }

      // Set up client port - if we have already had a list of valid ports, use it.
      if (hasValidClientPortInList(i)) {
        currentClientPort = clientPortList.get(i);
      } else {
        tentativePort = selectClientPort(tentativePort); // update the seed
        currentClientPort = tentativePort;
      }

      ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTimeToUse);
      // Setting {min,max}SessionTimeout defaults to be the same as in Zookeeper
      server.setMinSessionTimeout(configuration.getInt("hbase.zookeeper.property.minSessionTimeout", -1));
      server.setMaxSessionTimeout(configuration.getInt("hbase.zookeeper.property.maxSessionTimeout", -1));
      ServerCnxnFactory standaloneServerFactory;
      while (true) {
        try {
          standaloneServerFactory = createServerCnxnFactory(currentClientPort);
        } catch (BindException e) {
          LOG.debug("Failed binding ZK Server to client port: " + currentClientPort, e);
          // We're told to use some port but it's occupied, fail
          if (hasValidClientPortInList(i)) {
            return -1;
          }
          // This port is already in use, try to use another.
          tentativePort = selectClientPort(tentativePort);
          currentClientPort = tentativePort;
          continue;
        }
        break;
      }

      // Start up this ZK server
      standaloneServerFactory.startup(server);
      // Runs a 'stat' against the servers.
      if (!sslEnabled && !waitForServerUp(currentClientPort, connectionTimeout)) {
        throw new IOException("Waiting for startup of standalone server");
      }

      // We have selected a port as a client port.  Update clientPortList if necessary.
      if (clientPortList.size() <= i) { // it is not in the list, add the port
        clientPortList.add(currentClientPort);
      } else if (clientPortList.get(i) <= 0) { // the list has invalid port, update with valid port
        clientPortList.remove(i);
        clientPortList.add(i, currentClientPort);
      }

      standaloneServerFactoryList.add(standaloneServerFactory);
      zooKeeperServers.add(server);
    }

    // set the first one to be active ZK; Others are backups
    activeZKServerIndex = 0;
    started = true;
    int clientPort = clientPortList.get(activeZKServerIndex);
    LOG.info("Started MiniZooKeeperCluster and ran successful 'stat' " + "on client port=" + clientPort);
    return clientPort;
  }

  private ServerCnxnFactory createServerCnxnFactory(int currentClientPort) throws IOException {
    ServerCnxnFactory serverCnxnFactory = null;
    if (sslEnabled) {
      System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY,
          "org.apache.zookeeper.server.NettyServerCnxnFactory");
      String dataFileDir = !System.getProperty("test.data.files", "").isEmpty() ?
              System.getProperty("test.data.files") :
              configuration.get("test.data.files").replace('\\', '/').replace("c:", "");
      X509Util x509Util = new ClientX509Util();
      System.setProperty(x509Util.getSslKeystoreLocationProperty(),
          dataFileDir + File.separator + LOCALHOST_KEY_STORE_NAME);
      System.setProperty(x509Util.getSslKeystorePasswdProperty(),
          KEY_STORE_TRUST_STORE_PASSWORD);
      System.setProperty(x509Util.getSslTruststoreLocationProperty(),
          dataFileDir + File.separator + TRUST_STORE_NAME);
      System.setProperty(x509Util.getSslTruststorePasswdProperty(),
          KEY_STORE_TRUST_STORE_PASSWORD);
      serverCnxnFactory = ServerCnxnFactory.createFactory();
      serverCnxnFactory.configure(new InetSocketAddress(currentClientPort),
          configuration.getInt(HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS, HConstants.DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS),
          -1,true);
    } else {
      serverCnxnFactory = ServerCnxnFactory.createFactory();
      serverCnxnFactory.configure(new InetSocketAddress(currentClientPort),
          configuration.getInt(HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS, HConstants.DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS));
    }
    return serverCnxnFactory;
  }


  private void createDir(File dir) throws IOException {
    try {
      if (!dir.exists()) {
        dir.mkdirs();
      }
    } catch (SecurityException e) {
      throw new IOException("creating dir: " + dir, e);
    }
  }

  /**
   * @throws IOException
   */
  public void shutdown() throws IOException {
    // shut down all the zk servers
    for (int i = 0; i < standaloneServerFactoryList.size(); i++) {
      ServerCnxnFactory standaloneServerFactory = standaloneServerFactoryList.get(i);
      int clientPort = clientPortList.get(i);

      standaloneServerFactory.shutdown();
      if (!waitForServerDown(clientPort, connectionTimeout)) {
        throw new IOException("Waiting for shutdown of standalone server");
      }
    }
    standaloneServerFactoryList.clear();

    for (ZooKeeperServer zkServer : zooKeeperServers) {
      //explicitly close ZKDatabase since ZookeeperServer does not close them
      zkServer.getZKDatabase().close();
      zkServer.shutdown(true);
    }
    zooKeeperServers.clear();

    // clear everything
    if (started) {
      started = false;
      activeZKServerIndex = 0;
      clientPortList.clear();
      LOG.info("Shutdown MiniZK cluster with all ZK servers");
    }
  }

  /**@return clientPort return clientPort if there is another ZK backup can run
   *         when killing the current active; return -1, if there is no backups.
   * @throws IOException
   * @throws InterruptedException
   */
  public int killCurrentActiveZooKeeperServer() throws IOException, InterruptedException {
    if (!started || activeZKServerIndex < 0) {
      return -1;
    }

    // Shutdown the current active one
    ServerCnxnFactory standaloneServerFactory = standaloneServerFactoryList.get(activeZKServerIndex);
    int clientPort = clientPortList.get(activeZKServerIndex);

    standaloneServerFactory.shutdown();
    if (!waitForServerDown(clientPort, connectionTimeout)) {
      throw new IOException("Waiting for shutdown of standalone server");
    }

    zooKeeperServers.get(activeZKServerIndex).getZKDatabase().close();

    // remove the current active zk server
    standaloneServerFactoryList.remove(activeZKServerIndex);
    clientPortList.remove(activeZKServerIndex);
    zooKeeperServers.remove(activeZKServerIndex);
    LOG.info("Kill the current active ZK servers in the cluster " + "on client port: " + clientPort);

    if (standaloneServerFactoryList.isEmpty()) {
      // there is no backup servers;
      return -1;
    }
    clientPort = clientPortList.get(activeZKServerIndex);
    LOG.info("Activate a backup zk server in the cluster " + "on client port: " + clientPort);
    // return the next back zk server's port
    return clientPort;
  }

  /**
   * Kill one back up ZK servers
   * @throws IOException
   * @throws InterruptedException
   */
  public void killOneBackupZooKeeperServer() throws IOException, InterruptedException {
    if (!started || activeZKServerIndex < 0 || standaloneServerFactoryList.size() <= 1) {
      return;
    }

    int backupZKServerIndex = activeZKServerIndex + 1;
    // Shutdown the current active one
    ServerCnxnFactory standaloneServerFactory = standaloneServerFactoryList.get(backupZKServerIndex);
    int clientPort = clientPortList.get(backupZKServerIndex);

    standaloneServerFactory.shutdown();
    if (!waitForServerDown(clientPort, connectionTimeout)) {
      throw new IOException("Waiting for shutdown of standalone server");
    }

    zooKeeperServers.get(backupZKServerIndex).getZKDatabase().close();

    // remove this backup zk server
    standaloneServerFactoryList.remove(backupZKServerIndex);
    clientPortList.remove(backupZKServerIndex);
    zooKeeperServers.remove(backupZKServerIndex);
    LOG.info("Kill one backup ZK servers in the cluster " + "on client port: " + clientPort);
  }

  // XXX: From o.a.zk.t.ClientBase
  public static boolean waitForServerDown(int port, long timeout) throws IOException {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        Socket sock = new Socket("localhost", port);
        try {
          OutputStream outstream = sock.getOutputStream();
          outstream.write("stat".getBytes());
          outstream.flush();
        } finally {
          sock.close();
        }
      } catch (IOException e) {
        return true;
      }

      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      }
    }
    return false;
  }

  // XXX: From o.a.zk.t.ClientBase
  public static boolean waitForServerUp(int port, long timeout) throws IOException {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        Socket sock = new Socket("localhost", port);
        BufferedReader reader = null;
        try {
          OutputStream outstream = sock.getOutputStream();
          outstream.write("stat".getBytes());
          outstream.flush();

          Reader isr = new InputStreamReader(sock.getInputStream());
          reader = new BufferedReader(isr);
          String line = reader.readLine();
          if (line != null && line.startsWith("Zookeeper version:")) {
            return true;
          }
        } finally {
          sock.close();
          if (reader != null) {
            reader.close();
          }
        }
      } catch (IOException e) {
        // ignore as this is expected
        LOG.info("server localhost:" + port + " not up " + e);
      }

      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      }
    }
    return false;
  }

  public int getClientPort() {
    return activeZKServerIndex < 0 || activeZKServerIndex >= clientPortList.size() ? -1 : clientPortList.get(activeZKServerIndex);
  }
}
