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
package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.EventCleanerTask;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaStoreTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreTestUtils.class);
  public static final int RETRY_COUNT = 10;

  public static int startMetaStore() throws Exception {
    return MetaStoreTestUtils.startMetaStore(HadoopThriftAuthBridge.getBridge(), null);
  }

  public static int startMetaStore(final HadoopThriftAuthBridge bridge, Configuration conf)
      throws Exception {
    int port = MetaStoreTestUtils.findFreePort();
    MetaStoreTestUtils.startMetaStore(port, bridge, conf);
    return port;
  }

  public static int startMetaStore(Configuration conf) throws Exception {
    return startMetaStore(HadoopThriftAuthBridge.getBridge(), conf);
  }


  public static void startMetaStore(final int port, final HadoopThriftAuthBridge bridge) throws Exception {
    MetaStoreTestUtils.startMetaStore(port, bridge, null);
  }

  public static void startMetaStore(final int port,
      final HadoopThriftAuthBridge bridge, Configuration conf)
      throws Exception{
    if (conf == null) {
      conf = MetastoreConf.newMetastoreConf();
    }
    final Configuration finalConf = conf;
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          HiveMetaStore.startMetaStore(port, bridge, finalConf);
        } catch (Throwable e) {
          LOG.error("Metastore Thrift Server threw an exception...", e);
        }
      }
    });
    thread.setDaemon(true);
    thread.start();
    MetaStoreTestUtils.loopUntilHMSReady(port);
  }

  public static int startMetaStoreWithRetry(final HadoopThriftAuthBridge bridge) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(bridge, null);
  }

  public static int startMetaStoreWithRetry(Configuration conf) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
  }

  public static int startMetaStoreWithRetry() throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), null);
  }

  public static int startMetaStoreWithRetry(final HadoopThriftAuthBridge bridge, Configuration conf)
      throws Exception {
    int metaStorePort = findFreePort();
    startMetaStoreWithRetry(metaStorePort, bridge, conf);
    return metaStorePort;
  }

  private static void startMetaStoreWithRetry(int port, HadoopThriftAuthBridge bridge,
                                             Configuration conf) throws Exception {
    Exception metaStoreException = null;
    for (int tryCount = 0; tryCount < MetaStoreTestUtils.RETRY_COUNT; tryCount++) {
      try {
        MetaStoreTestUtils.startMetaStore(port, bridge, conf);
        return;
      } catch (ConnectException ce) {
        metaStoreException = ce;
      }
    }
    throw metaStoreException;
  }

  /**
   * A simple connect test to make sure that the metastore is up
   * @throws Exception
   */
  public static void loopUntilHMSReady(int port) throws Exception {
    int retries = 0;
    Exception exc = null;
    while (true) {
      try {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(port), 5000);
        socket.close();
        return;
      } catch (Exception e) {
        if (retries++ > 60) { //give up
          exc = e;
          break;
        }
        Thread.sleep(1000);
      }
    }
    // something is preventing metastore from starting
    // print the stack from all threads for debugging purposes
    LOG.error("Unable to connect to metastore server: " + exc.getMessage());
    LOG.info("Printing all thread stack traces for debugging before throwing exception.");
    LOG.info(MetaStoreTestUtils.getAllThreadStacksAsString());
    throw exc;
  }

  public static String getAllThreadStacksAsString() {
    Map<Thread, StackTraceElement[]> threadStacks = Thread.getAllStackTraces();
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Thread, StackTraceElement[]> entry : threadStacks.entrySet()) {
      Thread t = entry.getKey();
      sb.append(System.lineSeparator());
      sb.append("Name: ").append(t.getName()).append(" State: ").append(t.getState());
      MetaStoreTestUtils.addStackString(entry.getValue(), sb);
    }
    return sb.toString();
  }

  public static void addStackString(StackTraceElement[] stackElems, StringBuilder sb) {
    sb.append(System.lineSeparator());
    for (StackTraceElement stackElem : stackElems) {
      sb.append(stackElem).append(System.lineSeparator());
    }
  }

  /**
   * Finds a free port on the machine.
   *
   * @return
   * @throws IOException
   */
  public static int findFreePort() throws IOException {
    ServerSocket socket= new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  /**
   * Finds a free port on the machine, but allow the
   * ability to specify a port number to not use, no matter what.
   */
  public static int findFreePortExcepting(int portToExclude) throws IOException {
    ServerSocket socket1 = null;
    ServerSocket socket2 = null;
    try {
      socket1 = new ServerSocket(0);
      socket2 = new ServerSocket(0);
      if (socket1.getLocalPort() != portToExclude) {
        return socket1.getLocalPort();
      }
      // If we're here, then socket1.getLocalPort was the port to exclude
      // Since both sockets were open together at a point in time, we're
      // guaranteed that socket2.getLocalPort() is not the same.
      return socket2.getLocalPort();
    } finally {
      if (socket1 != null){
        socket1.close();
      }
      if (socket2 != null){
        socket2.close();
      }
    }
  }

  /**
   * Setup a configuration file for standalone mode.  There are a few config variables that have
   * defaults that require parts of Hive that aren't present in standalone mode.  This method
   * sets them to something that will work without the rest of Hive.  It only changes them if
   * they have not already been set, to avoid clobbering intentional changes.
   * @param conf Configuration object
   */
  public static void setConfForStandloneMode(Configuration conf) {
    if (MetastoreConf.getVar(conf, ConfVars.TASK_THREADS_ALWAYS).equals(
        ConfVars.TASK_THREADS_ALWAYS.getDefaultVal())) {
      MetastoreConf.setVar(conf, ConfVars.TASK_THREADS_ALWAYS,
          EventCleanerTask.class.getName());
    }
    if (MetastoreConf.getVar(conf, ConfVars.EXPRESSION_PROXY_CLASS).equals(
        ConfVars.EXPRESSION_PROXY_CLASS.getDefaultVal())) {
      MetastoreConf.setClass(conf, ConfVars.EXPRESSION_PROXY_CLASS,
          DefaultPartitionExpressionProxy.class, PartitionExpressionProxy.class);
    }
  }
}
