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
package org.apache.hive.hcatalog.templeton;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.security.UserGroupInformation;

public class UgiFactory {
  private static ConcurrentHashMap<String, UserGroupInformation> userUgiMap =
    new ConcurrentHashMap<String, UserGroupInformation>();

  public static UserGroupInformation getUgi(String user) throws IOException {
    UserGroupInformation ugi = userUgiMap.get(user);
    if (ugi == null) {
      //create new ugi and add to map
      final UserGroupInformation newUgi =
        UserGroupInformation.createProxyUser(user,
          UserGroupInformation.getLoginUser());

      //if another thread adds an entry before the check in this one
      // the one created here will not be added.
      userUgiMap.putIfAbsent(user, newUgi);

      //use the UGI object that got added
      return userUgiMap.get(user);

    }
    return ugi;
  }


}
