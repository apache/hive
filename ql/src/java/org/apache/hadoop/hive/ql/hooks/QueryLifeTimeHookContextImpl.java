/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.conf.HiveConf;


public class QueryLifeTimeHookContextImpl implements QueryLifeTimeHookContext {

  private HiveConf conf;
  private String command;
  private HookContext hc;

  @Override
  public HiveConf getHiveConf() {
    return conf;
  }

  @Override
  public void setHiveConf(HiveConf conf) {
    this.conf = conf;
  }

  @Override
  public String getCommand() {
    return command;
  }

  @Override
  public void setCommand(String command) {
    this.command = command;
  }

  @Override
  public HookContext getHookContext() {
    return hc;
  }

  @Override
  public void setHookContext(HookContext hc) {
    this.hc = hc;
  }

  public static class Builder {

    private HiveConf conf;
    private String command;
    private HookContext hc;

    public Builder withHiveConf(HiveConf conf) {
      this.conf = conf;
      return this;
    }

    public Builder withCommand(String command) {
      this.command = command;
      return this;
    }

    public Builder withHookContext(HookContext hc) {
      this.hc = hc;
      return this;
    }

    public QueryLifeTimeHookContextImpl build() {
      QueryLifeTimeHookContextImpl queryLifeTimeHookContext = new QueryLifeTimeHookContextImpl();
      queryLifeTimeHookContext.setHiveConf(this.conf);
      queryLifeTimeHookContext.setCommand(this.command);
      queryLifeTimeHookContext.setHookContext(this.hc);
      return queryLifeTimeHookContext;
    }
  }
}
