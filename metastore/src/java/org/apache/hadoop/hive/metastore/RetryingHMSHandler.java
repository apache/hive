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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RetryingHMSHandler implements InvocationHandler {

  private static final Log LOG = LogFactory.getLog(RetryingHMSHandler.class);

  private final IHMSHandler base;
  private final MetaStoreInit.MetaStoreInitData metaStoreInitData =
    new MetaStoreInit.MetaStoreInitData();

  private final HiveConf hiveConf;            // base configuration
  private final Configuration configuration;  // active configuration

  private RetryingHMSHandler(HiveConf hiveConf, String name, boolean local) throws MetaException {
    this.hiveConf = hiveConf;
    this.base = new HiveMetaStore.HMSHandler(name, hiveConf, false);
    if (local) {
      base.setConf(hiveConf); // tests expect configuration changes applied directly to metastore
    }
    configuration = base.getConf();

    // This has to be called before initializing the instance of HMSHandler
    // Using the hook on startup ensures that the hook always has priority
    // over settings in *.xml.  The thread local conf needs to be used because at this point
    // it has already been initialized using hiveConf.
    MetaStoreInit.updateConnectionURL(hiveConf, getConf(), null, metaStoreInitData);

    base.init();
  }

  public static IHMSHandler getProxy(HiveConf hiveConf, String name, boolean local)
      throws MetaException {

    RetryingHMSHandler handler = new RetryingHMSHandler(hiveConf, name, local);

    return (IHMSHandler) Proxy.newProxyInstance(
      RetryingHMSHandler.class.getClassLoader(),
      new Class[] { IHMSHandler.class }, handler);
  }

  @Override
  public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {

    boolean gotNewConnectUrl = false;
    boolean reloadConf = HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF);
    long retryInterval = HiveConf.getTimeVar(hiveConf,
        HiveConf.ConfVars.HMSHANDLERINTERVAL, TimeUnit.MILLISECONDS);
    int retryLimit = HiveConf.getIntVar(hiveConf,
        HiveConf.ConfVars.HMSHANDLERATTEMPTS);

    if (reloadConf) {
      MetaStoreInit.updateConnectionURL(hiveConf, getConf(),
        null, metaStoreInitData);
    }

    int retryCount = 0;
    Throwable caughtException = null;
    while (true) {
      try {
        if (reloadConf || gotNewConnectUrl) {
          base.setConf(getConf());
        }
        return method.invoke(base, args);

      } catch (javax.jdo.JDOException e) {
        caughtException = e;
      } catch (UndeclaredThrowableException e) {
        if (e.getCause() != null) {
          if (e.getCause() instanceof javax.jdo.JDOException) {
            // Due to reflection, the jdo exception is wrapped in
            // invocationTargetException
            caughtException = e.getCause();
          } else if (e.getCause() instanceof MetaException && e.getCause().getCause() != null
              && e.getCause().getCause() instanceof javax.jdo.JDOException) {
            // The JDOException may be wrapped further in a MetaException
            caughtException = e.getCause().getCause();
          } else {
            LOG.error(ExceptionUtils.getStackTrace(e.getCause()));
            throw e.getCause();
          }
        } else {
          LOG.error(ExceptionUtils.getStackTrace(e));
          throw e;
        }
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof javax.jdo.JDOException) {
          // Due to reflection, the jdo exception is wrapped in
          // invocationTargetException
          caughtException = e.getCause();
        } else if (e.getCause() instanceof NoSuchObjectException || e.getTargetException().getCause() instanceof NoSuchObjectException) {
          String methodName = method.getName();
          if (!methodName.startsWith("get_table") && !methodName.startsWith("get_partition") && !methodName.startsWith("get_function")) {
            LOG.error(ExceptionUtils.getStackTrace(e.getCause()));
          }
          throw e.getCause();
        } else if (e.getCause() instanceof MetaException && e.getCause().getCause() != null
            && e.getCause().getCause() instanceof javax.jdo.JDOException) {
          // The JDOException may be wrapped further in a MetaException
          caughtException = e.getCause().getCause();
        } else {
          LOG.error(ExceptionUtils.getStackTrace(e.getCause()));
          throw e.getCause();
        }
      }

      if (retryCount >= retryLimit) {
        LOG.error("HMSHandler Fatal error: " + ExceptionUtils.getStackTrace(caughtException));
        // Since returning exceptions with a nested "cause" can be a problem in
        // Thrift, we are stuffing the stack trace into the message itself.
        throw new MetaException(ExceptionUtils.getStackTrace(caughtException));
      }

      assert (retryInterval >= 0);
      retryCount++;
      LOG.error(
        String.format(
          "Retrying HMSHandler after %d ms (attempt %d of %d)", retryInterval, retryCount, retryLimit) +
          " with error: " + ExceptionUtils.getStackTrace(caughtException));

      Thread.sleep(retryInterval);
      // If we have a connection error, the JDO connection URL hook might
      // provide us with a new URL to access the datastore.
      String lastUrl = MetaStoreInit.getConnectionURL(getConf());
      gotNewConnectUrl = MetaStoreInit.updateConnectionURL(hiveConf, getConf(),
        lastUrl, metaStoreInitData);
    }
  }

  public Configuration getConf() {
    return configuration;
  }
}
