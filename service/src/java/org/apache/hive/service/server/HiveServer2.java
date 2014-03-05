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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionState;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;

/**
 * HiveServer2.
 *
 */
public class HiveServer2 extends CompositeService {
  private static final Log LOG = LogFactory.getLog(HiveServer2.class);

  private CLIService cliService;
  private ThriftCLIService thriftCLIService;

  public HiveServer2() {
    super("HiveServer2");
  }


  @Override
  public synchronized void init(HiveConf hiveConf) {
    cliService = new CLIService();
    addService(cliService);

    String transportMode = System.getenv("HIVE_SERVER2_TRANSPORT_MODE");
    if(transportMode == null) {
      transportMode = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    }
    if(transportMode != null && (transportMode.equalsIgnoreCase("http"))) {
      thriftCLIService = new ThriftHttpCLIService(cliService);
    }
    else {
      thriftCLIService = new ThriftBinaryCLIService(cliService);
    }

    addService(thriftCLIService);
    super.init(hiveConf);
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public synchronized void stop() {
    super.stop();
    // there should already be an instance of the session pool manager.
    // if not, ignoring is fine while stopping the hive server.
    HiveConf hiveConf = this.getHiveConf();
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS)) {
      try {
        TezSessionPoolManager.getInstance().stop();
      } catch (Exception e) {
        LOG.error("Tez session pool manager stop had an error during stop of hive server");
        e.printStackTrace();
      }
    }
  }

  private static void startHiveServer2() throws Throwable {
    long attempts = 0, maxAttempts = 1;
    while(true) {
      HiveConf hiveConf = new HiveConf();
      maxAttempts = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_START_ATTEMPTS);
      HiveServer2 server = null;
      try {
        server = new HiveServer2();
        server.init(hiveConf);
        server.start();
        if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS)) {
          TezSessionPoolManager sessionPool = TezSessionPoolManager.getInstance();
          sessionPool.setupPool(hiveConf);
          sessionPool.startPool();
        }
        break;
      } catch (Throwable throwable) {
        if(++attempts >= maxAttempts) {
          throw new Error("Max start attempts " + maxAttempts + " exhausted", throwable);
        } else {
          LOG.warn("Error starting HiveServer2 on attempt " + attempts +
            ", will retry in 60 seconds", throwable);
          try {
            if (server != null) {
              server.stop();
              server = null;
            }
          } catch (Exception e) {
            LOG.info("Exception caught when calling stop of HiveServer2 before" +
              " retrying start", e);
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
    try {
      ServerOptionsProcessor oproc = new ServerOptionsProcessor("hiveserver2");
      if (!oproc.process(args)) {
        System.err.println("Error starting HiveServer2 with given arguments");
        System.exit(-1);
      }

      //NOTE: It is critical to do this here so that log4j is reinitialized
      // before any of the other core hive classes are loaded
      String initLog4jMessage = LogUtils.initHiveLog4j();
      LOG.debug(initLog4jMessage);
      
      HiveStringUtils.startupShutdownMessage(HiveServer2.class, args, LOG);
      //log debug message from "oproc" after log4j initialize properly
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

