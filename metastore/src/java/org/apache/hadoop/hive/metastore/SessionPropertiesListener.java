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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;

import java.util.concurrent.TimeUnit;

/**
 * It handles the changed properties in the change event.
 */
public class SessionPropertiesListener extends MetaStoreEventListener {

  public SessionPropertiesListener(Configuration configuration) {
    super(configuration);
  }

  @Override
  public void onConfigChange(ConfigChangeEvent changeEvent) throws MetaException {
      if (changeEvent.getKey().equals(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname)) {
        Deadline.resetTimeout(HiveConf.toTime(changeEvent.getNewValue(), TimeUnit.SECONDS,
            TimeUnit.MILLISECONDS));
      }
  }
}
