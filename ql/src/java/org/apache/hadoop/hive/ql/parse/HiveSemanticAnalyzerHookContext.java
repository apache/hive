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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

/**
 * Context information provided by Hive to implementations of
 * HiveSemanticAnalyzerHook.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface HiveSemanticAnalyzerHookContext extends Configurable{
  /**
   * @return the Hive db instance; hook implementations can use this for
   * purposes such as getting configuration information or making metastore calls
   */
  public Hive getHive() throws HiveException;


  /**
   * This should be called after the semantic analyzer completes.
   * @param sem
   */
  public void update(BaseSemanticAnalyzer sem);


  /**
   * The following methods will only be available to hooks executing postAnalyze.  If called in a
   * preAnalyze method, they should return null.
   * @return the set of read entities
   */
  public Set<ReadEntity> getInputs();

  public Set<WriteEntity> getOutputs();

  public String getUserName();

  public void setUserName(String userName);

  public String getIpAddress();

  public void setIpAddress(String ipAddress);

  public String getCommand();

  public void setCommand(String command);

  public HiveOperation getHiveOperation();

  public void setHiveOperation(HiveOperation commandType);
}
