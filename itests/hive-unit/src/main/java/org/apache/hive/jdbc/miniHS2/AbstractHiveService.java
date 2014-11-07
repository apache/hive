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

package org.apache.hive.jdbc.miniHS2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

/***
 * Base class for Hive service
 * AbstractHiveService.
 *
 */
public abstract class AbstractHiveService {
  private HiveConf hiveConf = null;
  private String hostname;
  private int binaryPort;
  private int httpPort;
  private boolean startedHiveService = false;
  private List<String> addedProperties = new ArrayList<String>();

  public AbstractHiveService(HiveConf hiveConf, String hostname, int binaryPort, int httpPort) {
    this.hiveConf = hiveConf;
    this.hostname = hostname;
    this.binaryPort = binaryPort;
    this.httpPort = httpPort;
  }

  /**
   * Get Hive conf
   * @return
   */
  public HiveConf getHiveConf() {
    return hiveConf;
  }

  /**
   * Get config property
   * @param propertyKey
   * @return
   */
  public String getConfProperty(String propertyKey) {
    return hiveConf.get(propertyKey);
  }

  /**
   * Set config property
   * @param propertyKey
   * @param propertyValue
   */
  public void setConfProperty(String propertyKey, String propertyValue) {
    System.setProperty(propertyKey, propertyValue);
    hiveConf.set(propertyKey, propertyValue);
    addedProperties.add(propertyKey);
  }

  /**
   * Create system properties set by this server instance. This ensures that
   * the changes made by current test are not impacting subsequent tests.
   */
  public void clearProperties() {
    for (String propKey : addedProperties ) {
      System.clearProperty(propKey);
    }
  }

  /**
   * Retrieve warehouse directory
   * @return
   */
  public Path getWareHouseDir() {
    return new Path(hiveConf.getVar(ConfVars.METASTOREWAREHOUSE));
  }

  public void setWareHouseDir(String wareHouseURI) {
    verifyNotStarted();
    System.setProperty(ConfVars.METASTOREWAREHOUSE.varname, wareHouseURI);
    hiveConf.setVar(ConfVars.METASTOREWAREHOUSE, wareHouseURI);
  }

  /**
   * Set service host
   * @param hostName
   */
  public void setHost(String hostName) {
    this.hostname = hostName;
  }

  // get service host
  public String getHost() {
    return hostname;
  }

  /**
   * Set binary service port #
   * @param portNum
   */
  public void setBinaryPort(int portNum) {
    this.binaryPort = portNum;
  }

  /**
   * Set http service port #
   * @param portNum
   */
  public void setHttpPort(int portNum) {
    this.httpPort = portNum;
  }

  // Get binary service port #
  public int getBinaryPort() {
    return binaryPort;
  }

  // Get http service port #
  public int getHttpPort() {
    return httpPort;
  }

  public boolean isStarted() {
    return startedHiveService;
  }

  protected void setStarted(boolean hiveServiceStatus) {
    this.startedHiveService =  hiveServiceStatus;
  }

  protected void verifyStarted() {
    if (!isStarted()) {
      throw new IllegalStateException("HiveServer2 is not running");
    }
  }

  protected void verifyNotStarted() {
    if (isStarted()) {
      throw new IllegalStateException("HiveServer2 already running");
    }
  }

}
