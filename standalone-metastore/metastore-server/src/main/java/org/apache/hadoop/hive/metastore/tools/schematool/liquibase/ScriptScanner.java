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
package org.apache.hadoop.hive.metastore.tools.schematool.liquibase;

import org.apache.hadoop.hive.metastore.HiveMetaException;

import java.util.Set;

/**
 * Implementations must be able to scan scripts for table related statements, and list the tables a DB would have after
 * executing all the scripts in the provided path. The scanner must be aware of rename and drop commands.
 */
interface ScriptScanner {

  /**
   * Finds the table related commands in the script, and returns the list of tables which would be in the DB if the given 
   * script was run.
   * @param scriptPath Path to the script file
   * @param dbType The type of the Db the script is written for
   * @param tableList The set used to maintain the tables. This method either can add to it or remove from it              
   * @throws HiveMetaException Thrown when the script parsing fails.
   */
  void findTablesInScript(String scriptPath, String dbType, Set<String> tableList) throws HiveMetaException;

}
