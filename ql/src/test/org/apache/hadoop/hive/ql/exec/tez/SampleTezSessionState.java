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

package org.apache.hadoop.hive.ql.exec.tez;


import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
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
public class SampleTezSessionState extends WmTezSession {

  private boolean open;
  private final String sessionId;
  private final HiveConf hiveConf;
  private String user;
  private boolean doAsEnabled;
  private ListenableFuture<Boolean> waitForAmRegFuture;

  public SampleTezSessionState(
      String sessionId, TezSessionPoolSession.Manager parent, HiveConf conf) {
    super(sessionId, parent, (parent instanceof TezSessionPoolManager)
        ? ((TezSessionPoolManager)parent).getExpirationTracker() : null, conf);
    this.sessionId = sessionId;
    this.hiveConf = conf;
    waitForAmRegFuture = createDefaultWaitForAmRegistryFuture();
  }

  private SettableFuture<Boolean> createDefaultWaitForAmRegistryFuture() {
    SettableFuture<Boolean> noWait = SettableFuture.create();
    noWait.set(true); // By default, do not wait.
    return noWait;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  public void setOpen(boolean open) {
    this.open = open;
  }

  @Override
  public void open() throws LoginException, IOException {
    UserGroupInformation ugi = Utils.getUGI();
    user = ugi.getShortUserName();
    this.doAsEnabled = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);
    setOpen(true);
  }

  @Override
  public void open(HiveResources resources) throws LoginException, IOException {
    open();
  }

  @Override
  public void open(String[] additionalFiles) throws IOException, LoginException {
    open();
  }

  @Override
  void close(boolean keepTmpDir) throws TezException, IOException {
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

  @Override
  public SettableFuture<WmTezSession> waitForAmRegistryAsync(
      int timeoutMs, ScheduledExecutorService timeoutPool) {
    final SampleTezSessionState session = this;
    final SettableFuture<WmTezSession> future = SettableFuture.create();
    Futures.addCallback(waitForAmRegFuture, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(Boolean result) {
        future.set(session);
      }

      @Override
      public void onFailure(Throwable t) {
        future.setException(t);
      }
    });
    return future;
  }

  public void setWaitForAmRegistryFuture(ListenableFuture<Boolean> future) {
    waitForAmRegFuture = future != null ? future : createDefaultWaitForAmRegistryFuture();
  }
}
