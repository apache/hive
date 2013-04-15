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
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;

/**
 * HiveServer2.
 *
 */
public class HiveServer2 extends CompositeService {
  private static final Log LOG = LogFactory.getLog(HiveServer2.class);
  private static CompositeServiceShutdownHook serverShutdownHook;
  public static final int SHUTDOWN_HOOK_PRIORITY = 100;

  private CLIService cliService;
  private ThriftCLIService thriftCLIService;

  public HiveServer2() {
    super("HiveServer2");
  }


  @Override
  public synchronized void init(HiveConf hiveConf) {
    cliService = new CLIService();
    addService(cliService);

    thriftCLIService = new ThriftCLIService(cliService);
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
  }

  /**
   * @param args
   */
  public static void main(String[] args) {

    //NOTE: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    try {
      LogUtils.initHiveLog4j();
    } catch (LogInitializationException e) {
      LOG.warn(e.getMessage());
    }

    HiveStringUtils.startupShutdownMessage(HiveServer2.class, args, LOG);
    try {
      ServerOptionsProcessor oproc = new ServerOptionsProcessor("hiveserver2");
      if (!oproc.process(args)) {
        LOG.fatal("Error starting HiveServer2 with given arguments");
        System.exit(-1);
      }
      HiveConf hiveConf = new HiveConf();
      HiveServer2 server = new HiveServer2();
      server.init(hiveConf);
      server.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting HiveServer2", t);
      System.exit(-1);
    }
  }

}
