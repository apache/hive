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

package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * This factory creates the connection URL from the supplied configuration.
 *
 */

public class ConfUrlFactory implements ConnectionUrlFactory {

  HiveConf conf = null;
  String confVarName = null;
  public ConfUrlFactory() {
    this(new HiveConf(ConfUrlFactory.class), "");
  }

  public ConfUrlFactory(String confVarName) {
    this(new HiveConf(ConfUrlFactory.class), confVarName);
  }

  public ConfUrlFactory(HiveConf conf, String confVarName) {
    this.conf = conf;
    this.confVarName = confVarName;
  }

  public boolean init(Configuration hconf) {
    this.conf = (HiveConf)hconf;
    return true;
  }

  @Override
  public void init(String param1Name, String param2Name) {
    this.confVarName = param1Name;
  }

  @Override
  public String getUrl() throws Exception {
    String dbstr = conf.get(confVarName);
    String[] hostDatabases = dbstr.split(":");
    return "jdbc:mysql://" + hostDatabases[0] + "/" + hostDatabases[1];
  }

  @Override
  public String getUrl(boolean isWrite) throws Exception {
    return getUrl();
  }

  @Override
  public String getValue(String param1, String param2) throws Exception {
    return null;
  }

  @Override
  public void updateProps(String param1, String param2, String param3) {
    return;
  }
}
