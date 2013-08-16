/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.io.File;

public class Drone {

  private final String privateKey;
  private final String user;
  private final String host;
  private final int instance;
  private final String localDir;

  public Drone(String privateKey, String user, String host, int instance, String localDir) {
    this.privateKey = privateKey;
    this.user = user;
    this.host = host;
    this.instance = instance;
    this.localDir = localDir;
  }

  public String getPrivateKey() {
    return privateKey;
  }

  public String getUser() {
    return user;
  }

  public String getHost() {
    return host;
  }

  @Override
  public String toString() {
    return "Drone [user=" + user + ", host=" + host + ", instance=" + instance
        + "]";
  }
  public String getLocalDirectory() {
    return localDir;
  }
  public String getLocalLogDirectory() {
    return (new File(new File(localDir, getInstanceName()), "logs")).getAbsolutePath();
  }
  public String getInstanceName() {
    return String.format("%s-%s-%d", host, user, instance);
  }
  public int getInstance() {
    return instance;
  }
}
