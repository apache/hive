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
import java.util.List;

import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.util.ReflectionUtils;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RawStoreProxy implements InvocationHandler {

  private final RawStore base;
  private final MetaStoreInit.MetaStoreInitData metaStoreInitData =
    new MetaStoreInit.MetaStoreInitData();
  private final int id;
  private final HiveConf hiveConf;
  private final Configuration conf; // thread local conf from HMS

  protected RawStoreProxy(HiveConf hiveConf, Configuration conf,
      Class<? extends RawStore> rawStoreClass, int id) throws MetaException {
    this.conf = conf;
    this.hiveConf = hiveConf;
    this.id = id;

    // This has to be called before initializing the instance of RawStore
    init();

    this.base = ReflectionUtils.newInstance(rawStoreClass, conf);
  }

  public static RawStore getProxy(HiveConf hiveConf, Configuration conf, String rawStoreClassName,
      int id) throws MetaException {

    Class<? extends RawStore> baseClass = (Class<? extends RawStore>) MetaStoreUtils.getClass(
        rawStoreClassName);

    RawStoreProxy handler = new RawStoreProxy(hiveConf, conf, baseClass, id);

    // Look for interfaces on both the class and all base classes.
    return (RawStore) Proxy.newProxyInstance(RawStoreProxy.class.getClassLoader(),
        getAllInterfaces(baseClass), handler);
  }

  private static Class<?>[] getAllInterfaces(Class<?> baseClass) {
    List interfaces = ClassUtils.getAllInterfaces(baseClass);
    Class<?>[] result = new Class<?>[interfaces.size()];
    int i = 0;
    for (Object o : interfaces) {
      result[i++] = (Class<?>)o;
    }
    return result;
  }

  private void init() throws MetaException {
    // Using the hook on startup ensures that the hook always has priority
    // over settings in *.xml.  The thread local conf needs to be used because at this point
    // it has already been initialized using hiveConf.
    MetaStoreInit.updateConnectionURL(hiveConf, getConf(), null, metaStoreInitData);
  }

  private void initMS() {
    base.setConf(getConf());
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Object ret = null;

    try {
      ret = method.invoke(base, args);
    } catch (UndeclaredThrowableException e) {
      throw e.getCause();
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
    return ret;
  }

  public Configuration getConf() {
    return conf;
  }

}
