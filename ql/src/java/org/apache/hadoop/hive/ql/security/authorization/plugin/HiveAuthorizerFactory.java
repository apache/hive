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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;

/**
 * Implementation of this interface specified through hive configuration will be used to
 * create  {@link HiveAuthorizer} instance used for hive authorization.
 *
 */
@LimitedPrivate(value = { "" })
@Evolving
public interface HiveAuthorizerFactory {
  /**
   * Create a new instance of HiveAuthorizer, initialized with the given objects.
   * @param metastoreClientFactory - Use this to get the valid meta store client (IMetaStoreClient)
   *  for the current thread. Each invocation of method in HiveAuthorizer can happen in
   *  different thread, so get the current instance in each method invocation.
   * @param conf - current HiveConf
   * @param hiveAuthenticator - authenticator, provides user name
   * @return new instance of HiveAuthorizer
   * @throws HiveAuthzPluginException
   */
  HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, HiveAuthenticationProvider hiveAuthenticator) throws HiveAuthzPluginException;
}
