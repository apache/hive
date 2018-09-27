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

package org.apache.hive.service.cli.thrift;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.ICLIService;


/**
 * EmbeddedThriftBinaryCLIService.
 *
 */
public class EmbeddedThriftBinaryCLIService extends ThriftBinaryCLIService {

  public EmbeddedThriftBinaryCLIService() {
    // The non-test path that allows connections for the embedded service.
    super(new CLIService(null, true), null);
    isEmbedded = true;
    HiveConf.setLoadHiveServer2Config(true);
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    init(hiveConf, null);
  }

  public synchronized void init(HiveConf hiveConf, Map<String, String> confOverlay) {
    // Null HiveConf is passed in jdbc driver side code since driver side is supposed to be
    // independent of conf object. Create new HiveConf object here in this case.
    if (hiveConf == null) {
      hiveConf = new HiveConf();
    }
    // Set the specific parameters if needed
    if (confOverlay != null && !confOverlay.isEmpty()) {
      // apply overlay query specific settings, if any
      for (Map.Entry<String, String> confEntry : confOverlay.entrySet()) {
        try {
          hiveConf.set(confEntry.getKey(), confEntry.getValue());
        } catch (IllegalArgumentException e) {
          throw new RuntimeException("Error applying statement specific settings", e);
        }
      }
    }
    cliService.init(hiveConf);
    cliService.start();
    super.init(hiveConf);
  }

  public ICLIService getService() {
    return cliService;
  }
}
