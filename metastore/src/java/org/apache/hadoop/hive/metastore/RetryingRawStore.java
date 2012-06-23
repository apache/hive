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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.hooks.JDOConnectionURLHook;
import org.apache.hadoop.util.ReflectionUtils;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RetryingRawStore implements InvocationHandler {

  private static final Log LOG = LogFactory.getLog(RetryingRawStore.class);

  private final RawStore base;
  private int retryInterval = 0;
  private int retryLimit = 0;
  private JDOConnectionURLHook urlHook = null;
  private String urlHookClassName = "";
  private final int id;
  private final HiveConf hiveConf;
  private final Configuration conf; // thread local conf from HMS

  protected RetryingRawStore(HiveConf hiveConf, Configuration conf,
      Class<? extends RawStore> rawStoreClass, int id) throws MetaException {
    this.conf = conf;
    this.hiveConf = hiveConf;
    this.id = id;

    // This has to be called before initializing the instance of RawStore
    init();

    this.base = (RawStore) ReflectionUtils.newInstance(rawStoreClass, conf);
  }

  public static RawStore getProxy(HiveConf hiveConf, Configuration conf, String rawStoreClassName,
      int id) throws MetaException {

    Class<? extends RawStore> baseClass = (Class<? extends RawStore>) MetaStoreUtils.getClass(
        rawStoreClassName);

    RetryingRawStore handler = new RetryingRawStore(hiveConf, conf, baseClass, id);

    return (RawStore) Proxy.newProxyInstance(RetryingRawStore.class.getClassLoader()
        , baseClass.getInterfaces(), handler);
  }

  private void init() throws MetaException {
    retryInterval = HiveConf.getIntVar(hiveConf,
        HiveConf.ConfVars.METASTOREINTERVAL);
    retryLimit = HiveConf.getIntVar(hiveConf,
        HiveConf.ConfVars.METASTOREATTEMPTS);
    // Using the hook on startup ensures that the hook always has priority
    // over settings in *.xml.  The thread local conf needs to be used because at this point
    // it has already been initialized using hiveConf.
    updateConnectionURL(getConf(), null);
  }

  private void initMS() {
    base.setConf(getConf());
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Object ret = null;

    boolean gotNewConnectUrl = false;
    boolean reloadConf = HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.METASTOREFORCERELOADCONF);

    if (reloadConf) {
      updateConnectionURL(getConf(), null);
    }

    int retryCount = 0;
    Exception caughtException = null;
    while (true) {
      try {
        if (reloadConf || gotNewConnectUrl) {
          initMS();
        }
        ret = method.invoke(base, args);
        break;
      } catch (javax.jdo.JDOException e) {
        caughtException = e;
      } catch (UndeclaredThrowableException e) {
        throw e.getCause();
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof javax.jdo.JDOException) {
          // Due to reflection, the jdo exception is wrapped in
          // invocationTargetException
          caughtException = e;
        }
        else {
          throw e.getCause();
        }
      }

      if (retryCount >= retryLimit) {
        throw caughtException;
      }

      assert (retryInterval >= 0);
      retryCount++;
      LOG.error(
          String.format(
              "JDO datastore error. Retrying metastore command " +
                  "after %d ms (attempt %d of %d)", retryInterval, retryCount, retryLimit));
      Thread.sleep(retryInterval);
      // If we have a connection error, the JDO connection URL hook might
      // provide us with a new URL to access the datastore.
      String lastUrl = getConnectionURL(getConf());
      gotNewConnectUrl = updateConnectionURL(getConf(), lastUrl);
    }
    return ret;
  }

  /**
   * Updates the connection URL in hiveConf using the hook
   *
   * @return true if a new connection URL was loaded into the thread local
   *         configuration
   */
  private boolean updateConnectionURL(Configuration conf, String badUrl)
      throws MetaException {
    String connectUrl = null;
    String currentUrl = getConnectionURL(conf);
    try {
      // We always call init because the hook name in the configuration could
      // have changed.
      initConnectionUrlHook();
      if (urlHook != null) {
        if (badUrl != null) {
          urlHook.notifyBadConnectionUrl(badUrl);
        }
        connectUrl = urlHook.getJdoConnectionUrl(hiveConf);
      }
    } catch (Exception e) {
      LOG.error("Exception while getting connection URL from the hook: " +
          e);
    }

    if (connectUrl != null && !connectUrl.equals(currentUrl)) {
      LOG.error(addPrefix(
          String.format("Overriding %s with %s",
              HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(),
              connectUrl)));
      conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(),
          connectUrl);
      return true;
    }
    return false;
  }

  private static String getConnectionURL(Configuration conf) {
    return conf.get(
        HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(), "");
  }

  // Multiple threads could try to initialize at the same time.
  synchronized private void initConnectionUrlHook()
      throws ClassNotFoundException {

    String className =
        hiveConf.get(HiveConf.ConfVars.METASTORECONNECTURLHOOK.toString(), "").trim();
    if (className.equals("")) {
      urlHookClassName = "";
      urlHook = null;
      return;
    }
    boolean urlHookChanged = !urlHookClassName.equals(className);
    if (urlHook == null || urlHookChanged) {
      urlHookClassName = className.trim();

      Class<?> urlHookClass = Class.forName(urlHookClassName, true,
          JavaUtils.getClassLoader());
      urlHook = (JDOConnectionURLHook) ReflectionUtils.newInstance(urlHookClass, null);
    }
    return;
  }

  private String addPrefix(String s) {
    return id + ": " + s;
  }

  public Configuration getConf() {
    return conf;
  }

}
