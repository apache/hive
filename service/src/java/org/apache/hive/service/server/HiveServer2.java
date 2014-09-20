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

package org.apache.hive.service.server;

import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * HiveServer2.
 *
 */
public class HiveServer2 extends CompositeService {
  private static final Log LOG = LogFactory.getLog(HiveServer2.class);

  private CLIService cliService;
  private ThriftCLIService thriftCLIService;
  private String znodePath;
  private ZooKeeper zooKeeperClient;
  private boolean registeredWithZooKeeper = false;

  public HiveServer2() {
    super(HiveServer2.class.getSimpleName());
    HiveConf.setLoadHiveServer2Config(true);
  }


  @Override
  public synchronized void init(HiveConf hiveConf) {
    cliService = new CLIService();
    addService(cliService);
    if (isHTTPTransportMode(hiveConf)) {
      thriftCLIService = new ThriftHttpCLIService(cliService);
    } else {
      thriftCLIService = new ThriftBinaryCLIService(cliService);
    }
    addService(thriftCLIService);
    thriftCLIService.setHiveServer2(this);
    super.init(hiveConf);

    // Add a shutdown hook for catching SIGTERM & SIGINT
    final HiveServer2 hiveServer2 = this;
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        hiveServer2.stop();
      }
    });
  }

  public static boolean isHTTPTransportMode(HiveConf hiveConf) {
    String transportMode = System.getenv("HIVE_SERVER2_TRANSPORT_MODE");
    if (transportMode == null) {
      transportMode = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    }
    if (transportMode != null && (transportMode.equalsIgnoreCase("http"))) {
      return true;
    }
    return false;
  }

  /**
   * Adds a server instance to ZooKeeper as a znode.
   *
   * @param hiveConf
   * @throws Exception
   */
  private void addServerInstanceToZooKeeper(HiveConf hiveConf) throws Exception {
    int zooKeeperSessionTimeout =
        hiveConf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT);
    String zooKeeperEnsemble = ZooKeeperHiveHelper.getQuorumServers(hiveConf);
    String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
    String instanceURI = getServerInstanceURI(hiveConf);
    byte[] znodeDataUTF8 = instanceURI.getBytes(Charset.forName("UTF-8"));
    zooKeeperClient =
        new ZooKeeper(zooKeeperEnsemble, zooKeeperSessionTimeout,
            new ZooKeeperHiveHelper.DummyWatcher());

    // Create the parent znodes recursively; ignore if the parent already exists
    try {
      ZooKeeperHiveHelper.createPathRecursively(zooKeeperClient, rootNamespace,
          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      LOG.info("Created the root name space: " + rootNamespace + " on ZooKeeper for HiveServer2");
    } catch (KeeperException e) {
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        LOG.fatal("Unable to create HiveServer2 namespace: " + rootNamespace + " on ZooKeeper", e);
        throw (e);
      }
    }
    // Create a znode under the rootNamespace parent for this instance of the server
    // Znode name: server-host:port-versionInfo-sequence
    try {
      String znodePath =
          ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace
              + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + "server-" + instanceURI + "-"
              + HiveVersionInfo.getVersion() + "-";
      znodePath =
          zooKeeperClient.create(znodePath, znodeDataUTF8, Ids.OPEN_ACL_UNSAFE,
              CreateMode.EPHEMERAL_SEQUENTIAL);
      setRegisteredWithZooKeeper(true);
      // Set a watch on the znode
      if (zooKeeperClient.exists(znodePath, new DeRegisterWatcher()) == null) {
        // No node exists, throw exception
        throw new Exception("Unable to create znode for this HiveServer2 instance on ZooKeeper.");
      }
      LOG.info("Created a znode on ZooKeeper for HiveServer2 uri: " + instanceURI);
    } catch (KeeperException e) {
      LOG.fatal("Unable to create a znode for this server instance", e);
      throw new Exception(e);
    }
  }

  /**
   * The watcher class which sets the de-register flag when the znode corresponding to this server
   * instance is deleted. Additionally, it shuts down the server if there are no more active client
   * sessions at the time of receiving a 'NodeDeleted' notification from ZooKeeper.
   */
  private class DeRegisterWatcher implements Watcher {
    public void process(WatchedEvent event) {
      if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
        HiveServer2.this.setRegisteredWithZooKeeper(false);
        // If there are no more active client sessions, stop the server
        if (cliService.getSessionManager().getOpenSessionCount() == 0) {
          LOG.warn("This instance of HiveServer2 has been removed from the list of server "
              + "instances available for dynamic service discovery. "
              + "The last client session has ended - will shutdown now.");
          HiveServer2.this.stop();
        }
        LOG.warn("This HiveServer2 instance is now de-registered from ZooKeeper. "
            + "The server will be shut down after the last client sesssion completes.");
      }
    }
  }

  private void removeServerInstanceFromZooKeeper() throws Exception {
    setRegisteredWithZooKeeper(false);
    zooKeeperClient.close();
    LOG.info("Server instance removed from ZooKeeper.");
  }

  public boolean isRegisteredWithZooKeeper() {
    return registeredWithZooKeeper;
  }

  private void setRegisteredWithZooKeeper(boolean registeredWithZooKeeper) {
    this.registeredWithZooKeeper = registeredWithZooKeeper;
  }

  private String getServerInstanceURI(HiveConf hiveConf) throws Exception {
    if ((thriftCLIService == null) || (thriftCLIService.getServerAddress() == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.");
    }
    return thriftCLIService.getServerAddress().getHostName() + ":"
        + thriftCLIService.getPortNumber();
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public synchronized void stop() {
    LOG.info("Shutting down HiveServer2");
    HiveConf hiveConf = this.getHiveConf();
    super.stop();
    // Remove this server instance from ZooKeeper if dynamic service discovery is set
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY)) {
      try {
        removeServerInstanceFromZooKeeper();
      } catch (Exception e) {
        LOG.error("Error removing znode for this HiveServer2 instance from ZooKeeper.", e);
      }
    }
    // There should already be an instance of the session pool manager.
    // If not, ignoring is fine while stopping HiveServer2.
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS)) {
      try {
        TezSessionPoolManager.getInstance().stop();
      } catch (Exception e) {
        LOG.error("Tez session pool manager stop had an error during stop of HiveServer2. "
            + "Shutting down HiveServer2 anyway.", e);
      }
    }

    if (hiveConf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
      try {
        SparkSessionManagerImpl.getInstance().shutdown();
      } catch(Exception ex) {
        LOG.error("Spark session pool manager failed to stop during HiveServer2 shutdown.", ex);
      }
    }
  }

  private static void startHiveServer2() throws Throwable {
    long attempts = 0, maxAttempts = 1;
    while (true) {
      HiveConf hiveConf = new HiveConf();
      maxAttempts = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_START_ATTEMPTS);
      HiveServer2 server = null;
      try {
        server = new HiveServer2();
        server.init(hiveConf);
        server.start();
        // If we're supporting dynamic service discovery, we'll add the service uri for this
        // HiveServer2 instance to Zookeeper as a znode.
        if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY)) {
          server.addServerInstanceToZooKeeper(hiveConf);
        }
        if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS)) {
          TezSessionPoolManager sessionPool = TezSessionPoolManager.getInstance();
          sessionPool.setupPool(hiveConf);
          sessionPool.startPool();
        }

        if (hiveConf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
          SparkSessionManagerImpl.getInstance().setup(hiveConf);
        }
        break;
      } catch (Throwable throwable) {
        if (++attempts >= maxAttempts) {
          throw new Error("Max start attempts " + maxAttempts + " exhausted", throwable);
        } else {
          LOG.warn("Error starting HiveServer2 on attempt " + attempts
              + ", will retry in 60 seconds", throwable);
          try {
            if (server != null) {
              server.stop();
              server = null;
            }
          } catch (Exception e) {
            LOG.info(
                "Exception caught when calling stop of HiveServer2 before" + " retrying start", e);
          }
          try {
            Thread.sleep(60L * 1000L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  public static void main(String[] args) {
    HiveConf.setLoadHiveServer2Config(true);
    try {
      ServerOptionsProcessor oproc = new ServerOptionsProcessor("hiveserver2");
      if (!oproc.process(args)) {
        System.err.println("Error starting HiveServer2 with given arguments");
        System.exit(-1);
      }

      // NOTE: It is critical to do this here so that log4j is reinitialized
      // before any of the other core hive classes are loaded
      String initLog4jMessage = LogUtils.initHiveLog4j();
      LOG.debug(initLog4jMessage);

      HiveStringUtils.startupShutdownMessage(HiveServer2.class, args, LOG);
      // log debug message from "oproc" after log4j initialize properly
      LOG.debug(oproc.getDebugMessage().toString());

      startHiveServer2();
    } catch (LogInitializationException e) {
      LOG.error("Error initializing log: " + e.getMessage(), e);
      System.exit(-1);
    } catch (Throwable t) {
      LOG.fatal("Error starting HiveServer2", t);
      System.exit(-1);
    }
  }
}

