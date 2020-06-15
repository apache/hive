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

import java.lang.reflect.InvocationTargetException;

import javax.security.sasl.AuthenticationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This authentication provider implements the {@code CUSTOM} authentication. It allows a {@link
 * MetaStorePasswdAuthenticationProvider} to be specified at configuration time which may
 * additionally
 * implement {@link org.apache.hadoop.conf.Configurable Configurable} to grab Metastore's {@link
 * org.apache.hadoop.conf.Configuration Configuration}.
 */
// This file is copy of org.apache.hive.service.auth.AuthenticationProviderFactory. Need to
//  deduplicate this code.
public class MetaStoreCustomAuthenticationProviderImpl implements MetaStorePasswdAuthenticationProvider {
  private final MetaStorePasswdAuthenticationProvider customProvider;
  protected static final Logger LOG = LoggerFactory.getLogger(MetaStoreCustomAuthenticationProviderImpl.class);

  @SuppressWarnings("unchecked")
  MetaStoreCustomAuthenticationProviderImpl(Configuration conf) throws AuthenticationException {
    String customProviderName = MetastoreConf.getVar(conf,
            MetastoreConf.ConfVars.METASTORE_CUSTOM_AUTHENTICATION_CLASS);
    Class<? extends MetaStorePasswdAuthenticationProvider> customHandlerClass;
    try {
      customHandlerClass = JavaUtils.getClass(customProviderName,
              MetaStorePasswdAuthenticationProvider.class);
    } catch (MetaException me) {
      throw new AuthenticationException(me.getMessage());
    }
    MetaStorePasswdAuthenticationProvider customProvider;
    try {
      customProvider = customHandlerClass.getConstructor(Configuration.class).newInstance(conf);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      customProvider = ReflectionUtils.newInstance(customHandlerClass, conf);
    }
    this.customProvider = customProvider;
  }

  @Override
  public void authenticate(String user, String password) throws AuthenticationException {
    customProvider.authenticate(user, password);
  }

}
