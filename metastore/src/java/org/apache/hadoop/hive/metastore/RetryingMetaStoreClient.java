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

package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

/**
 * RetryingMetaStoreClient. Creates a proxy for a IMetaStoreClient
 * implementation and retries calls to it on failure.
 * If the login user is authenticated using keytab, it relogins user before
 * each call.
 *
 */
public class RetryingMetaStoreClient implements InvocationHandler {

  private static final Log LOG = LogFactory.getLog(RetryingMetaStoreClient.class.getName());

  private final IMetaStoreClient base;
  private final int retryLimit;
  private final long retryDelaySeconds;



  protected RetryingMetaStoreClient(HiveConf hiveConf, HiveMetaHookLoader hookLoader,
      Class<? extends IMetaStoreClient> msClientClass) throws MetaException {
    this.retryLimit = hiveConf.getIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES);
    this.retryDelaySeconds = hiveConf.getTimeVar(
        HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);

    reloginExpiringKeytabUser();
    this.base = MetaStoreUtils.newInstance(msClientClass, new Class[] {
        HiveConf.class, HiveMetaHookLoader.class}, new Object[] {hiveConf, hookLoader});
  }

  public static IMetaStoreClient getProxy(HiveConf hiveConf, HiveMetaHookLoader hookLoader,
      String mscClassName) throws MetaException {

    Class<? extends IMetaStoreClient> baseClass = (Class<? extends IMetaStoreClient>)
        MetaStoreUtils.getClass(mscClassName);

    RetryingMetaStoreClient handler = new RetryingMetaStoreClient(hiveConf, hookLoader, baseClass);

    return (IMetaStoreClient) Proxy.newProxyInstance(RetryingMetaStoreClient.class.getClassLoader(),
        baseClass.getInterfaces(), handler);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Object ret = null;
    int retriesMade = 0;
    TException caughtException = null;
    while (true) {
      try {
        reloginExpiringKeytabUser();
        if(retriesMade > 0){
          base.reconnect();
        }
        ret = method.invoke(base, args);
        break;
      } catch (UndeclaredThrowableException e) {
        throw e.getCause();
      } catch (InvocationTargetException e) {
        if ((e.getCause() instanceof TApplicationException) ||
            (e.getCause() instanceof TProtocolException) ||
            (e.getCause() instanceof TTransportException)) {
          caughtException = (TException) e.getCause();
        } else if ((e.getCause() instanceof MetaException) &&
            e.getCause().getMessage().matches("(?s).*JDO[a-zA-Z]*Exception.*")) {
          caughtException = (MetaException) e.getCause();
        } else {
          throw e.getCause();
        }
      }

      if (retriesMade >=  retryLimit) {
        throw caughtException;
      }
      retriesMade++;
      LOG.warn("MetaStoreClient lost connection. Attempting to reconnect.",
          caughtException);
      Thread.sleep(retryDelaySeconds * 1000);
    }
    return ret;
  }

  /**
   * Relogin if login user is logged in using keytab
   * Relogin is actually done by ugi code only if sufficient time has passed
   * A no-op if kerberos security is not enabled
   * @throws MetaException
   */
  private void reloginExpiringKeytabUser() throws MetaException {
    if(!UserGroupInformation.isSecurityEnabled()){
      return;
    }
    try {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      //checkTGT calls ugi.relogin only after checking if it is close to tgt expiry
      //hadoop relogin is actually done only every x minutes (x=10 in hadoop 1.x)
      if(ugi.isFromKeytab()){
        ugi.checkTGTAndReloginFromKeytab();
      }
    } catch (IOException e) {
      String msg = "Error doing relogin using keytab " + e.getMessage();
      LOG.error(msg, e);
      throw new MetaException(msg);
    }
  }

}
