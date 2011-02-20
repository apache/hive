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

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * After getting a contextInitialized event this component starts an instance of
 * the HiveSessionManager.
 * 
 */
public class HWIContextListener implements javax.servlet.ServletContextListener {

  protected static final Log l4j = LogFactory.getLog(HWIContextListener.class
      .getName());

  /**
   * The Hive Web Interface manages multiple hive sessions. This event is used
   * to start a Runnable, HiveSessionManager as a thread inside the servlet
   * container.
   * 
   * @param sce
   *          An event fired by the servlet context on startup
   */
  public void contextInitialized(ServletContextEvent sce) {
    ServletContext sc = sce.getServletContext();
    HWISessionManager hs = new HWISessionManager();
    l4j.debug("HWISessionManager created.");
    Thread t = new Thread(hs);
    t.start();
    l4j.debug("HWISessionManager thread started.");
    sc.setAttribute("hs", hs);
    l4j.debug("HWISessionManager placed in application context.");
  }

  /**
   * When the Hive Web Interface is closing we locate the Runnable
   * HiveSessionManager and set it's internal goOn variable to false. This
   * should allow the application to gracefully shutdown.
   * 
   * @param sce
   *          An event fired by the servlet context on context shutdown
   */
  public void contextDestroyed(ServletContextEvent sce) {
    ServletContext sc = sce.getServletContext();
    HWISessionManager hs = (HWISessionManager) sc.getAttribute("hs");
    if (hs == null) {
      l4j.error("HWISessionManager was not found in context");
    } else {
      l4j.error("HWISessionManager goOn set to false. Shutting down.");
      hs.setGoOn(false);
    }
  }
}
