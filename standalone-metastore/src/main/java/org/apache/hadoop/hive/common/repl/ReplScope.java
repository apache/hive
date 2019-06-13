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
package org.apache.hadoop.hive.common.repl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Class that stores the replication scope. Replication scope includes the details of database and
 * tables included under the scope of replication.
 */
public class ReplScope implements Serializable {
  private String dbName;
  private Pattern dbNamePattern;
  private List<Pattern> includedTableNamePatterns; // Only for REPL DUMP and exist only if tableName == null.
  private List<Pattern> excludedTableNamePatterns; // Only for REPL DUMP and exist only if tableName == null.

  public ReplScope() {
  }

  public ReplScope(String dbName) {
    setDbName(dbName);
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
    this.dbNamePattern = (((dbName == null) || "*".equals(dbName))
            ? null : Pattern.compile(dbName, Pattern.CASE_INSENSITIVE));
  }

  public String getDbName() {
    return dbName;
  }

  public void setIncludedTablePatterns(List<String> includedTableNamePatterns) {
    this.includedTableNamePatterns = compilePatterns(includedTableNamePatterns);
  }

  public void setExcludedTablePatterns(List<String> excludedTableNamePatterns) {
    this.excludedTableNamePatterns = compilePatterns(excludedTableNamePatterns);
  }

  public boolean includeAllTables() {
    return ((includedTableNamePatterns == null)
            && (excludedTableNamePatterns == null));
  }

  public boolean includedInReplScope(final String dbName, final String tableName) {
    return dbIncludedInReplScope(dbName) && tableIncludedInReplScope(tableName);
  }

  public boolean dbIncludedInReplScope(final String dbName) {
    return (dbNamePattern == null) || dbNamePattern.matcher(dbName).matches();
  }

  public boolean tableIncludedInReplScope(final String tableName) {
    if (tableName == null) {
      // If input tableName is empty, it means, DB level event. It should be always included as
      // this is DB level replication with list of included/excluded tables.
      return true;
    }
    return (inTableIncludedList(tableName) && !inTableExcludedList(tableName));
  }

  private List<Pattern> compilePatterns(List<String> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return null;
    }
    List<Pattern> compiledPatterns = new ArrayList<>();
    for (String pattern : patterns) {
      // Convert the pattern to lower case because events/HMS will have table names in lower case.
      compiledPatterns.add(Pattern.compile(pattern, Pattern.CASE_INSENSITIVE));
    }
    return compiledPatterns;
  }

  private boolean tableMatchAnyPattern(final String tableName, List<Pattern> tableNamePatterns) {
    assert(tableNamePatterns != null);
    for (Pattern tableNamePattern : tableNamePatterns) {
      if (tableNamePattern.matcher(tableName).matches()) {
        return true;
      }
    }
    return false;
  }

  private boolean inTableIncludedList(final String tableName) {
    if (includedTableNamePatterns == null) {
      // If included list is empty, repl policy should be full db replication.
      // So, all tables must be included.
      return true;
    }
    return tableMatchAnyPattern(tableName, includedTableNamePatterns);
  }

  private boolean inTableExcludedList(final String tableName) {
    if (excludedTableNamePatterns == null) {
      // If excluded tables list is empty means, all tables in included list must be accepted.
      return false;
    }
    return tableMatchAnyPattern(tableName, excludedTableNamePatterns);
  }
}
