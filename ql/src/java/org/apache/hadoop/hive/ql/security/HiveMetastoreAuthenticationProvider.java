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

package org.apache.hadoop.hive.ql.security;

import org.apache.hadoop.hive.metastore.IHMSHandler;


/**
 * HiveMetastoreAuthenticationProvider is an interface extension
 * from HiveAuthenticationProvider for authentication from the
 * metastore side. The implementation should return userNames
 * and groupNames, and take care that if the metastore is running
 * a particular command as a user, it returns that user.
 */
public interface HiveMetastoreAuthenticationProvider extends HiveAuthenticationProvider{

  /**
   * Allows invoker of HiveMetastoreAuthenticationProvider to send in a
   * hive metastore handler that can be used to provide data for any
   * authentication that needs to be done.
   * @param handler
   */
  void setMetaStoreHandler(IHMSHandler handler);

}
