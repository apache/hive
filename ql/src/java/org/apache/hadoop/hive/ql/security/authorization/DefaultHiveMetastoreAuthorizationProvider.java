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

package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class DefaultHiveMetastoreAuthorizationProvider extends BitSetCheckedAuthorizationProvider
  implements HiveMetastoreAuthorizationProvider {

  @Override
  public void init(Configuration conf) throws HiveException {
    hive_db = new HiveProxy();
  }

  @Override
  public void setMetaStoreHandler(HMSHandler handler) {
    hive_db.setHandler(handler);
  }

  @Override
  public void authorizeAuthorizationApiInvocation() throws HiveException, AuthorizationException {
    // default no-op implementation
  }


}
