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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreInit.MetaStoreInitData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

public abstract class AbstractHMSHandlerProxy implements InvocationHandler {
  protected final MetaStoreInitData metaStoreInitData = new MetaStoreInitData();
  protected final IHMSHandler baseHandler;
  protected final Configuration origConf;    // base configuration
  protected final Configuration activeConf;  // active configuration
  protected final boolean reloadConf;
  protected final long timeout;

  public AbstractHMSHandlerProxy(Configuration conf, IHMSHandler baseHandler, boolean local)
      throws MetaException {
    this.origConf = conf;
    this.baseHandler = baseHandler;
    if (local) {
      baseHandler.setConf(origConf); // tests expect configuration changes applied directly to metastore
    }
    activeConf = baseHandler.getConf();
    reloadConf = MetastoreConf.getBoolVar(origConf, ConfVars.HMS_HANDLER_FORCE_RELOAD_CONF);
    timeout = MetastoreConf.getTimeVar(origConf,
            ConfVars.CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

    // This has to be called before initializing the instance of HMSHandler
    // Using the hook on startup ensures that the hook always has priority
    // over settings in *.xml.  The thread local conf needs to be used because at this point
    // it has already been initialized using hiveConf.
    MetaStoreInit.updateConnectionURL(origConf, getActiveConf(), null, metaStoreInitData);
    initBaseHandler();
  }

  static final class Result {
    static final Result ERROR_RESULT = new Result(null, "error=true");
    private final Object result;
    private final String additionalInfo;

    public Result(Object result, String additionalInfo) {
      this.result = result;
      this.additionalInfo = additionalInfo;
    }
  }

  protected void initBaseHandler() throws MetaException {
    baseHandler.init();
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] objects) throws Throwable {
    PerfLogger perfLogger = PerfLogger.getPerfLogger(false);
    perfLogger.perfLogBegin(this.getClass().getName(), method.getName());
    Result result = Result.ERROR_RESULT;
    Deadline.registerIfNot(timeout);
    try {
      result = invokeInternal(proxy, method, objects);
      return result == null ? null : result.result;
    } finally {
      String info = result == null ? "" : result.additionalInfo;
      perfLogger.perfLogEnd(this.getClass().getName(), method.getName(), info);
    }
  }

  protected abstract Result invokeInternal(Object proxy, Method method, Object[] args)
      throws Throwable;

  public Configuration getActiveConf() {
    return activeConf;
  }
}
