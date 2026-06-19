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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * ProxyUserAuthenticator extends HadoopDefaultAuthenticator
 * but honours a proxy config setting proxy.user.name instead of the
 * current user if set. This allows server processes like webhcat which
 * proxy other users to easily specify an override if allowed.
 *
 * It is no longer necessary to use this class with WebHCat as of Hive 0.14.
 */
public class ProxyUserAuthenticator extends HadoopDefaultAuthenticator {

  private static final String PROXY_USER_NAME = "proxy.user.name";

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    UserGroupInformation ugi = null;
    String proxyUser = conf.get(PROXY_USER_NAME);

    if (proxyUser == null){
      super.setConf(conf);
      return;
    }

    // If we're here, proxy user is set.

    try {
      ugi = UserGroupInformation.createRemoteUser(proxyUser);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (ugi == null) {
      throw new RuntimeException(
          "Can not initialize ProxyUserAuthenticator for user ["+proxyUser+"]");
    }

    this.userName = ugi.getShortUserName();
    if (ugi.getGroupNames() != null) {
      this.groupNames = Arrays.asList(ugi.getGroupNames());
    }
  }

}
