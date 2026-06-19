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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

/**
 * The helper class for all the Templeton delegator classes. A
 * delegator will call the underlying Templeton service such as hcat
 * or hive.
 */
public class TempletonDelegator {
  /**
   * http://hadoop.apache.org/docs/r1.0.4/commands_manual.html#Generic+Options
   */
  public static final String ARCHIVES = "-archives";
  /**
   * http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options
   */
  public static final String FILES = "-files";
  
  protected AppConfig appConf;

  public TempletonDelegator(AppConfig appConf) {
    this.appConf = appConf;
  }
}
