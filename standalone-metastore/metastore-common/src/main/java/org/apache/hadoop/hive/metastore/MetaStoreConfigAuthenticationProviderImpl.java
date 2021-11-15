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

import javax.security.sasl.AuthenticationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This authentication provider implements the {@code CONFIG} authentication. It allows a {@link
 * MetaStorePasswdAuthenticationProvider} to be specified at configuration time which may
 * additionally
 * implement {@link org.apache.hadoop.conf.Configurable Configurable} to grab HMS's {@link
 * org.apache.hadoop.conf.Configuration Configuration}.
 */
public class MetaStoreConfigAuthenticationProviderImpl implements MetaStorePasswdAuthenticationProvider {
  private final String userName;
  private final String password;
  protected static final Logger LOG = LoggerFactory.getLogger(MetaStoreConfigAuthenticationProviderImpl.class);

  @SuppressWarnings("unchecked")
  MetaStoreConfigAuthenticationProviderImpl(Configuration conf) throws AuthenticationException {
    userName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.THRIFT_AUTH_CONFIG_USERNAME);
    password = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.THRIFT_AUTH_CONFIG_PASSWORD);

    if (null == userName || userName.isEmpty()) {
      throw new AuthenticationException("No username specified in " +
              MetastoreConf.ConfVars.THRIFT_AUTH_CONFIG_USERNAME);
    }

    if (null == password) {
      throw new AuthenticationException("No password specified in " +
              MetastoreConf.ConfVars.THRIFT_AUTH_CONFIG_PASSWORD);
    }
  }

  @Override
  public void authenticate(String authUser, String authPassword) throws AuthenticationException {
    if (!userName.equals(authUser)) {
      LOG.debug("Invalid user " + authUser);
      throw new AuthenticationException("Invalid credentials");
    }

    if (!password.equals(authPassword)) {
      LOG.debug("Invalid password for user " + authUser);
      throw new AuthenticationException("Invalid credentials");
    }

    LOG.debug("User " + authUser + " successfully authenticated.");
  }
}
