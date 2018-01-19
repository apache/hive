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

package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * HiveMetastoreAuthorizationProvider : An extension of HiveAuthorizaytionProvider
 * that is intended to be called from the metastore-side. It will be invoked
 * by AuthorizationPreEventListener.
 *
 */
public interface HiveMetastoreAuthorizationProvider extends HiveAuthorizationProvider {

  /**
   * Allows invoker of HiveMetaStoreAuthorizationProvider to send in a
   * hive metastore handler that can be used to make calls to test
   * whether or not authorizations can/will succeed. Intended to be called
   * before any of the authorize methods are called.
   * @param handler
   */
  void setMetaStoreHandler(IHMSHandler handler);

  /**
   * Authorize metastore authorization api call.
   */
  void authorizeAuthorizationApiInvocation() throws HiveException, AuthorizationException;


}
