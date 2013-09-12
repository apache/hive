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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.storagehandler;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;

/**
 * This class is a dummy implementation of HiveAuthorizationProvider to provide
 * dummy authorization functionality for other classes to extend and override.
 */
class DummyHCatAuthProvider implements HiveAuthorizationProvider {

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public void setConf(Configuration conf) {
  }

  /*
  * (non-Javadoc)
  *
  * @see
  * org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider
  * #init(org.apache.hadoop.conf.Configuration)
  */
  @Override
  public void init(Configuration conf) throws HiveException {
  }

  @Override
  public HiveAuthenticationProvider getAuthenticator() {
    return null;
  }

  @Override
  public void setAuthenticator(HiveAuthenticationProvider authenticator) {
  }

  /*
  * (non-Javadoc)
  *
  * @see
  * org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider
  * #authorize(org.apache.hadoop.hive.ql.security.authorization.Privilege[],
  * org.apache.hadoop.hive.ql.security.authorization.Privilege[])
  */
  @Override
  public void authorize(Privilege[] readRequiredPriv,
              Privilege[] writeRequiredPriv) throws HiveException,
    AuthorizationException {
  }

  /*
  * (non-Javadoc)
  *
  * @see
  * org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider
  * #authorize(org.apache.hadoop.hive.metastore.api.Database,
  * org.apache.hadoop.hive.ql.security.authorization.Privilege[],
  * org.apache.hadoop.hive.ql.security.authorization.Privilege[])
  */
  @Override
  public void authorize(Database db, Privilege[] readRequiredPriv,
              Privilege[] writeRequiredPriv) throws HiveException,
    AuthorizationException {
  }

  /*
  * (non-Javadoc)
  *
  * @see
  * org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider
  * #authorize(org.apache.hadoop.hive.ql.metadata.Table,
  * org.apache.hadoop.hive.ql.security.authorization.Privilege[],
  * org.apache.hadoop.hive.ql.security.authorization.Privilege[])
  */
  @Override
  public void authorize(Table table, Privilege[] readRequiredPriv,
              Privilege[] writeRequiredPriv) throws HiveException,
    AuthorizationException {
  }

  /*
  * (non-Javadoc)
  *
  * @see
  * org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider
  * #authorize(org.apache.hadoop.hive.ql.metadata.Partition,
  * org.apache.hadoop.hive.ql.security.authorization.Privilege[],
  * org.apache.hadoop.hive.ql.security.authorization.Privilege[])
  */
  @Override
  public void authorize(Partition part, Privilege[] readRequiredPriv,
              Privilege[] writeRequiredPriv) throws HiveException,
    AuthorizationException {
  }

  /*
  * (non-Javadoc)
  *
  * @see
  * org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider
  * #authorize(org.apache.hadoop.hive.ql.metadata.Table,
  * org.apache.hadoop.hive.ql.metadata.Partition, java.util.List,
  * org.apache.hadoop.hive.ql.security.authorization.Privilege[],
  * org.apache.hadoop.hive.ql.security.authorization.Privilege[])
  */
  @Override
  public void authorize(Table table, Partition part, List<String> columns,
              Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
    throws HiveException, AuthorizationException {
  }

}
