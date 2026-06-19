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

package org.apache.hadoop.hive.ql.ddl.privilege;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationTranslator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationTranslator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveV1Authorizer;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Common utilities for Privilege related ddl operations.
 */
public final class PrivilegeUtils {
  private PrivilegeUtils() {
    throw new UnsupportedOperationException("PrivilegeUtils should not be instantiated");
  }

  public static HiveAuthorizer getSessionAuthorizer(HiveConf conf) {
    HiveAuthorizer authorizer = SessionState.get().getAuthorizerV2();
    if (authorizer == null) {
      authorizer = new HiveV1Authorizer(conf);
    }

    return authorizer;
  }

  public static void writeListToFileAfterSort(List<String> entries, String resFile, DDLOperationContext context)
      throws IOException {
    Collections.sort(entries);

    StringBuilder sb = new StringBuilder();
    for (String entry : entries) {
      ShowUtils.appendNonNull(sb, entry, true);
    }

    ShowUtils.writeToFile(sb.toString(), resFile, context);
  }

  private static final HiveAuthorizationTranslator DEFAULT_AUTHORIZATION_TRANSLATOR =
      new DefaultHiveAuthorizationTranslator();

  public static HiveAuthorizationTranslator getAuthorizationTranslator(HiveAuthorizer authorizer)
      throws HiveAuthzPluginException {
    if (authorizer.getHiveAuthorizationTranslator() == null) {
      return DEFAULT_AUTHORIZATION_TRANSLATOR;
    } else {
      return (HiveAuthorizationTranslator)authorizer.getHiveAuthorizationTranslator();
    }
  }
}
