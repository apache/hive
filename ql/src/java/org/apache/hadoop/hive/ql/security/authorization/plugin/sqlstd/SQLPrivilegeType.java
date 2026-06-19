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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import java.util.Locale;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;

public enum SQLPrivilegeType {
  //ALL privilege is expanded to these, so it is not needed here
  SELECT, INSERT, UPDATE, DELETE;

  public static SQLPrivilegeType getRequirePrivilege(String priv)
      throws HiveAuthzPluginException {
    SQLPrivilegeType reqPriv;
    if(priv == null){
      throw new HiveAuthzPluginException("Null privilege obtained");
    }
    try {
      reqPriv = SQLPrivilegeType.valueOf(priv.toUpperCase(Locale.US));
    } catch (IllegalArgumentException e) {
      throw new HiveAuthzPluginException("Unsupported privilege type " + priv, e);
    }
    return reqPriv;
  }


};
