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

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.net.URISyntaxException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.dag.api.TezException;


/**
 * This class is needed for writing junit tests. For testing the multi-session
 * use case from hive server 2, we need a session simulation.
 *
 */
public class SampleTezSessionState extends TezSessionState {

  private boolean open;
  private final String sessionId;
  private HiveConf hiveConf;
  private String user;
  private boolean doAsEnabled;

  public SampleTezSessionState(String sessionId) {
    super(sessionId);
    this.sessionId = sessionId;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  public void setOpen(boolean open) {
    this.open = open;
  }

  @Override
  public void open(HiveConf conf) throws IOException, LoginException, URISyntaxException,
      TezException {
    this.hiveConf = conf;
    UserGroupInformation ugi = Utils.getUGI();
    user = ugi.getShortUserName();
    this.doAsEnabled = conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);
  }

  @Override
  public void close(boolean keepTmpDir) throws TezException, IOException {
    open = keepTmpDir;
  }

  @Override
  public HiveConf getConf() {
    return this.hiveConf;
  }

  @Override
  public String getSessionId() {
    return sessionId;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public boolean getDoAsEnabled() {
    return this.doAsEnabled;
  }
}
