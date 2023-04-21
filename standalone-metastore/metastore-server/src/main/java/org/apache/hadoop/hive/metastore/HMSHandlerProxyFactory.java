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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;

import java.lang.reflect.Proxy;

public class HMSHandlerProxyFactory {
  public static IHMSHandler getProxy(Configuration conf, IHMSHandler handler, boolean local)
      throws MetaException {
    String hmsHandlerProxyName = MetastoreConf.getVar(conf, ConfVars.HMS_HANDLER_PROXY_CLASS);
    Class<? extends AbstractHMSHandlerProxy> proxyClass =
            JavaUtils.getClass(hmsHandlerProxyName, AbstractHMSHandlerProxy.class);
    AbstractHMSHandlerProxy invacationHandler = JavaUtils.newInstance(proxyClass,
        new Class[]{Configuration.class, IHMSHandler.class, boolean.class},
            new Object[]{conf, handler, local});

    return (IHMSHandler) Proxy.newProxyInstance(
            HMSHandlerProxyFactory.class.getClassLoader(),
            new Class[]{ IHMSHandler.class }, invacationHandler);
  }
}
