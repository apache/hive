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
package org.apache.hive.hcatalog.cli.SemanticAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

final class HCatAuthUtil {
  public static boolean isAuthorizationEnabled(Configuration conf) {
    // the session state getAuthorizer can return null even if authorization is
    // enabled if the V2 api of authorizer in use.
    // The additional authorization checks happening in hcatalog are designed to
    // work with  storage based authorization (on client side). It should not try doing
    // additional checks if a V2 authorizer is in use. The reccomended configuration is to
    // use storage based authorization in metastore server
    return HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)
        && SessionState.get().getAuthorizer() != null;
  }
}
