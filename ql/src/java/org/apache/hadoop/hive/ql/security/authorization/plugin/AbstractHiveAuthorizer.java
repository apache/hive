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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

/**
 * Abstract class that extends HiveAuthorizer. This will help to shield
 * Hive authorization implementations from some of the changes to HiveAuthorizer
 * interface by providing default implementation of new methods in HiveAuthorizer
 * when possible.
 */
public abstract class AbstractHiveAuthorizer implements HiveAuthorizer {

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer#getHiveAuthorizationTranslator()
   */
  @Override
  public HiveAuthorizationTranslator getHiveAuthorizationTranslator() throws HiveAuthzPluginException {
    // No customization of this API is done for most Authorization implementations. It is meant 
    // to be used for special cases in Apache Sentry (incubating)
    // null is to be returned when no customization is needed for the translator
    // see javadoc in interface for details.
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer#
   * getHivePolicyProvider()
   */
  @Override
  public HivePolicyProvider getHivePolicyProvider() throws HiveAuthzPluginException {
    return null;
  }

}
