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

package org.apache.hadoop.hive.ql.parse;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

public class HiveSemanticAnalyzerHookContextImpl implements HiveSemanticAnalyzerHookContext {

  Configuration conf;
  Set<ReadEntity> inputs = null;
  Set<WriteEntity> outputs = null;
  private String userName;
  private String ipAddress;
  private String command;
  private HiveOperation commandType;

  @Override
  public Hive getHive() throws HiveException {

    return Hive.get((HiveConf)conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void update(BaseSemanticAnalyzer sem) {
    this.inputs = sem.getInputs();
    this.outputs = sem.getOutputs();
    this.commandType = sem.getQueryState().getHiveOperation();
  }

  @Override
  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  @Override
  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  @Override
  public String getIpAddress() {
    return ipAddress;
  }

  @Override
  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
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
  public HiveOperation getHiveOperation() {
    return commandType;
  }

  @Override
  public void setHiveOperation(HiveOperation commandType) {
    this.commandType = commandType;
  }
}
