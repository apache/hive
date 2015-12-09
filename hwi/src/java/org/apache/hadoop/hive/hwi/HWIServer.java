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

package org.apache.hadoop.hive.hwi;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.JettyShims;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * This is the entry point for HWI. A web server is invoked in the same manner
 * as the hive CLI. Rather then opening a command line session a web server is
 * started and a web application to work with hive is started.
 */
public class HWIServer {
  protected static final Logger l4j = LoggerFactory.getLogger(HWIServer.class.getName());

  private JettyShims.Server webServer;
  private final String[] args;

  /**
   * 
   * @param args
   *          These are the command line arguments. Usually -hiveconf.
   * @throws java.io.IOException
   */
  public HWIServer(String[] args) throws IOException {
    this.args = args;
  }

  /**
   * This method initialized the internal Jetty Servlet Engine. It adds the hwi
   * context path.
   * 
   * @throws java.io.IOException
   *           Port already in use, bad bind etc.
   */
  public void start() throws IOException {

    HiveConf conf = new HiveConf(this.getClass());

    String listen = null;
    int port = -1;

    listen = conf.getVar(HiveConf.ConfVars.HIVEHWILISTENHOST);
    port = conf.getIntVar(HiveConf.ConfVars.HIVEHWILISTENPORT);

    if (listen.equals("")) {
      l4j.warn("hive.hwi.listen.host was not specified defaulting to 0.0.0.0");
      listen = "0.0.0.0";
    }
    if (port == -1) {
      l4j.warn("hive.hwi.listen.port was not specified defaulting to 9999");
      port = 9999;
    }

    String hwiWAR = conf.getVar(HiveConf.ConfVars.HIVEHWIWARFILE);
    String hivehome = System.getenv().get("HIVE_HOME");
    File hwiWARFile = new File(hivehome, hwiWAR);
    if (!hwiWARFile.exists()) {
      l4j.error("HWI WAR file not found at " + hwiWARFile.toString());
      System.exit(1);
    }

    webServer = ShimLoader.getJettyShims().startServer(listen, port);
    webServer.addWar(hwiWARFile.toString(), "/hwi");

    /*
     * The command line args may be used by multiple components. Rather by
     * setting these as a system property we avoid having to specifically pass
     * them
     */
    StringBuilder sb = new StringBuilder();
    for (String arg : args) {
      sb.append(arg + " ");
    }
    System.setProperty("hwi-args", sb.toString());

    try {
      while (true) {
        try {
          webServer.start();
          webServer.join();
          l4j.debug(" HWI Web Server is started.");
          break;
        } catch (org.mortbay.util.MultiException ex) {
          throw ex;
        }
      }
    } catch (IOException ie) {
      throw ie;
    } catch (Exception e) {
      IOException ie = new IOException("Problem starting HWI server");
      ie.initCause(e);
      l4j.error("Parsing hwi.listen.port caused exception ", e);
      throw ie;
    }
  }

  /**
   * 
   * @param args
   *          as of now no arguments are supported
   * @throws java.lang.Exception
   *           Could be thrown if due to issues with Jetty or bad configuration
   *           options
   * 
   */
  public static void main(String[] args) throws Exception {
    HWIServer hwi = new HWIServer(args);
    l4j.info("HWI is starting up");
    hwi.start();
  }

  /**
   * Shut down the running HWI Server.
   * 
   * @throws Exception
   *           Running Thread.stop() can and probably will throw this
   */
  public void stop() throws Exception {
    l4j.info("HWI is shutting down");
    webServer.stop();
  }

}
