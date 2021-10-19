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
package org.apache.hadoop.hive.ql.qoption;

import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.externalDB.AbstractExternalDB;
import org.apache.hadoop.hive.ql.externalDB.MySQLExternalDB;
import org.apache.hadoop.hive.ql.externalDB.PostgresExternalDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class QTestExternalDBHandler implements QTestOptionHandler {
  /**
   * Properties accepted by the handler. 
   */
  private enum Property {
    /**
     * The database type.
     */
    TYPE,
    /**
     * The name of the initialization script.
     */
    SCRIPT,
    /**
     * The reusability status of the database.
     */
    REUSE
  }
  private String dbType;
  private String initScript;

  @Override
  public void processArguments(String arguments) {
    String[] args = arguments.split(":");
    dbType = args[0].toLowerCase();
    initScript = args[1];
//    Map<Property, String> args = new HashMap<>();
//    for(String p: pairs) {
//      String[] propValue = p.split("=");
//      if(propValue.length != 2) {
//        throw new IllegalArgumentException("Illegal property format:" + p);
//      }
//      args.put(Property.valueOf(propValue[0].toUpperCase()), propValue[1]);
//    }
//    args.get(Property.TYPE)
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    if(dbType == null) {
      return;
    }
    Path externalDBScript = Paths.get(qt.getScriptsDir(), dbType, initScript);

    AbstractExternalDB abstractExternalDB = createDB(dbType);
    abstractExternalDB.launchDockerContainer();
    if (Files.exists(externalDBScript)) {
      abstractExternalDB.execute(externalDBScript.toString());
    }
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    if(dbType == null) {
      return;
    }
    AbstractExternalDB abstractExternalDB = createDB(dbType);
    abstractExternalDB.cleanupDockerContainer();
  }

  private static AbstractExternalDB createDB(String dbType) {
    switch (dbType) {
    case "postgres":
      return new PostgresExternalDB();
    case "mysql":
      return new MySQLExternalDB();
    default:
      throw new IllegalArgumentException("Unsupported database type: " + dbType);
    }
  }
}
