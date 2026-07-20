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
package org.apache.hadoop.hive.metastore.tools.schematool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
@RunWith(Parameterized.class)
public class TestRebuildIndexesScriptConsistency {

  private static final Pattern POSTGRES_PATTERN = Pattern.compile(
      "CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?\"?([A-Za-z0-9_]+)\"?",
      Pattern.CASE_INSENSITIVE);

  // MySQL indexes show up both as standalone CREATE INDEX and as inline KEY clauses inside CREATE TABLE.
  // The negative lookahead prevents matching "PRIMARY KEY AUTO_INCREMENT".
  private static final Pattern MYSQL_PATTERN = Pattern.compile(
      "(?:CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+|(?:UNIQUE\\s+)?KEY\\s+(?!AUTO_INCREMENT\\b))`?([A-Za-z0-9_]+)`?",
      Pattern.CASE_INSENSITIVE);

  private static final Pattern ORACLE_PATTERN = Pattern.compile(
      "CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+\"?([A-Za-z0-9_]+)\"?",
      Pattern.CASE_INSENSITIVE);

  private static final Pattern MSSQL_PATTERN = Pattern.compile(
      "CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+\"?([A-Za-z0-9_]+)\"?",
      Pattern.CASE_INSENSITIVE);

  private static final Map<String, Pattern> PATTERNS = new HashMap<>();
  static {
    PATTERNS.put(HiveSchemaHelper.DB_POSTGRES, POSTGRES_PATTERN);
    PATTERNS.put(HiveSchemaHelper.DB_MYSQL, MYSQL_PATTERN);
    PATTERNS.put(HiveSchemaHelper.DB_ORACLE, ORACLE_PATTERN);
    PATTERNS.put(HiveSchemaHelper.DB_MSSQL, MSSQL_PATTERN);
  }

  private static final Pattern DROP_PATTERN = Pattern.compile(
      "DROP\\s+INDEX\\s+(?:IF\\s+EXISTS\\s+)?[`\"']?([A-Za-z0-9_]+)[`\"']?",
      Pattern.CASE_INSENSITIVE);

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> dbTypes() {
    return Arrays.asList(
        HiveSchemaHelper.DB_POSTGRES,
        HiveSchemaHelper.DB_MYSQL,
        HiveSchemaHelper.DB_ORACLE,
        HiveSchemaHelper.DB_MSSQL);
  }

  private final String dbType;

  public TestRebuildIndexesScriptConsistency(String dbType) {
    this.dbType = dbType;
  }

  @Test
  public void rebuildScriptMatchesInitScript() throws Exception {
    String metastoreHome = System.getProperty("test.tmp.dir", "target/tmp");
    IMetaStoreSchemaInfo schemaInfo =
        MetaStoreSchemaInfoFactory.get(new Configuration(), metastoreHome, dbType);

    String scriptDir = schemaInfo.getMetaStoreScriptDir();
    String version = schemaInfo.getHiveSchemaVersion();

    File schemaScript = new File(scriptDir, "hive-schema-" + version + "." + dbType + ".sql");
    File rebuildScript = new File(scriptDir,
        SchemaToolTaskRebuildIndexes.REBUILD_INDEXES_FILE_PREFIX + "." + dbType + ".sql");

    assertTrue("Schema script not found: " + schemaScript, schemaScript.exists());
    assertTrue("Rebuild script not found: " + rebuildScript, rebuildScript.exists());

    Pattern pattern = PATTERNS.get(dbType);
    Set<String> initIndexes = extractIndexNames(schemaScript, pattern);
    Set<String> rebuildIndexes = extractIndexNames(rebuildScript, pattern);

    Set<String> missing = new HashSet<>(initIndexes);
    missing.removeAll(rebuildIndexes);

    Set<String> extra = new HashSet<>(rebuildIndexes);
    extra.removeAll(initIndexes);

    assertTrue("Indexes in init script missing from rebuild script: " + missing, missing.isEmpty());
    assertTrue("Indexes in rebuild script not found in init script: " + extra, extra.isEmpty());
  }

  @Test
  public void rebuildScriptHasDropAndCreateForEachIndex() throws Exception {
    String metastoreHome = System.getProperty("test.tmp.dir", "target/tmp");
    IMetaStoreSchemaInfo schemaInfo =
        MetaStoreSchemaInfoFactory.get(new Configuration(), metastoreHome, dbType);

    File rebuildScript = new File(schemaInfo.getMetaStoreScriptDir(),
        SchemaToolTaskRebuildIndexes.REBUILD_INDEXES_FILE_PREFIX + "." + dbType + ".sql");

    assertTrue("Rebuild script not found: " + rebuildScript, rebuildScript.exists());

    List<String> lines = Files.readAllLines(rebuildScript.toPath());
    Set<String> created = extractIndexNames(lines, PATTERNS.get(dbType));
    Set<String> dropped = extractIndexNames(lines, DROP_PATTERN);

    Set<String> createdButNotDropped = new HashSet<>(created);
    createdButNotDropped.removeAll(dropped);

    Set<String> droppedButNotCreated = new HashSet<>(dropped);
    droppedButNotCreated.removeAll(created);

    assertTrue("Indexes created but never dropped: " + createdButNotDropped, createdButNotDropped.isEmpty());
    assertTrue("Indexes dropped but never created: " + droppedButNotCreated, droppedButNotCreated.isEmpty());
    assertEquals("Mismatch between number of DROPs and CREATEs", dropped.size(), created.size());
  }

  private Set<String> extractIndexNames(File file, Pattern pattern) throws IOException {
    return extractIndexNames(Files.readAllLines(file.toPath()), pattern);
  }

  private Set<String> extractIndexNames(List<String> lines, Pattern pattern) {
    Set<String> names = new HashSet<>();
    for (String line : lines) {
      if (line.trim().startsWith("--")) {
        continue;
      }
      Matcher m = pattern.matcher(line);
      if (m.find()) {
        names.add(m.group(1).toUpperCase());
      }
    }
    return names;
  }
}

