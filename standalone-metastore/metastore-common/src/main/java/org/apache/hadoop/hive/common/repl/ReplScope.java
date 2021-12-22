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
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Class that stores the replication scope. Replication scope includes the details of database and
 * tables included under the scope of replication.
 */
public class ReplScope implements Serializable {
  private String dbName;
  private Pattern dbNamePattern;

  // Include and exclude table names/patterns exist only for REPL DUMP.
  private String includedTableNames;
  private String excludedTableNames;
  private Pattern includedTableNamePattern;
  private Pattern excludedTableNamePattern;

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

  public void setIncludedTablePatterns(String includedTableNames) {
    this.includedTableNames = includedTableNames;
    this.includedTableNamePattern = compilePattern(includedTableNames);
  }

  public String getIncludedTableNames() {
    return includedTableNames;
  }

  public void setExcludedTablePatterns(String excludedTableNames) {
    this.excludedTableNames = excludedTableNames;
    this.excludedTableNamePattern = compilePattern(excludedTableNames);
  }

  public String getExcludedTableNames() {
    return excludedTableNames;
  }

  public boolean includeAllTables() {
    return ((includedTableNamePattern == null)
            && (excludedTableNamePattern == null));
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

  private Pattern compilePattern(String pattern) {
    if (pattern == null) {
      return null;
    }

    // Convert the pattern to lower case because events/HMS will have table names in lower case.
    return Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
  }

  private boolean inTableIncludedList(final String tableName) {
    if (includedTableNamePattern == null) {
      // If included list is empty, repl policy should be full db replication.
      // So, all tables must be included.
      return true;
    }
    return includedTableNamePattern.matcher(tableName).matches();
  }

  private boolean inTableExcludedList(final String tableName) {
    if (excludedTableNamePattern == null) {
      // If excluded tables list is empty means, all tables in included list must be accepted.
      return false;
    }
    return excludedTableNamePattern.matcher(tableName).matches();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (! (o instanceof ReplScope)) {
      return false;
    }

    ReplScope replScope = (ReplScope)o;
    return Objects.equals(replScope.excludedTableNames, this.excludedTableNames)
      && Objects.equals(replScope.includedTableNames, this.includedTableNames)
      && Objects.equals(replScope.dbName, this.dbName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dbName, dbNamePattern, includedTableNames, excludedTableNames);
  }
}
