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

package org.apache.hive.jdbc.saml;

import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.hive.jdbc.saml.IJdbcBrowserClient.HiveJdbcBrowserException;

/**
 * Factory class to instantiate the {@link IJdbcBrowserClient}. This is mostly used for
 * testing purposes so that test can instantiate a test browser client which can do
 * browser interaction programmatically.
 */
public interface IJdbcBrowserClientFactory {

  /**
   * Create a {@link IJdbcBrowserClient} from a the given {@link JdbcConnectionParams}
   * @throws HiveJdbcBrowserException In case of any error to instantiate the browser
   * client.
   */
  IJdbcBrowserClient create(JdbcConnectionParams connectionParams)
      throws HiveJdbcBrowserException;
}
