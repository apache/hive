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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.HiveMetaException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This {@link ScriptScanner} implementation call can scan Liquibase XML format changesets for tables.
 */
class XmlScriptScanner implements ScriptScanner {

  private final Pattern pattern = Pattern.compile("<createTable(?:.|\\s)*?tableName=\"(.+?)\"|<renameTable(?:.|\\s)*?oldTableName=\"(.+?)\"(?:.|\\s)*?newTableName=\"(.+?)\"|<dropTable(?:.|\\s)*?tableName=\"(.+?)\"");

  /**
   * @param dbType not in use
   */
  @Override
  public void findTablesInScript(String scriptPath, String dbType, Set<String> tableList) throws HiveMetaException {
    try {
      String content = new String(Files.readAllBytes(Paths.get(scriptPath)));
      Matcher matcher = pattern.matcher(content);
      while (matcher.find()) {
        if (StringUtils.isNotBlank(matcher.group(1))) { // create table
          tableList.add(matcher.group(1).toLowerCase());
        } else if (StringUtils.isNotBlank(matcher.group(2))) { // rename table
          tableList.remove(matcher.group(2).toLowerCase());
          tableList.add(matcher.group(3).toLowerCase());
        } else if (StringUtils.isNotBlank(matcher.group(4))) { // drop table
          tableList.remove(matcher.group(4).toLowerCase());
        }
      }
    } catch (IOException e) {
      throw new HiveMetaException("Unable to load and parse script file: " + scriptPath, e);
    }
  }
}
