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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class SqlScriptScanner implements ScriptScanner {

  private static final Logger LOG = LoggerFactory.getLogger(SqlScriptScanner.class);

  private final Pattern createPattern = Pattern.compile("(?i)(?:^|;)\\s*CREATE TABLE(?:\\s+IF NOT EXISTS)?\\s+(\\S+).*");
  private final Pattern dropPattern = Pattern.compile("(?i)(?:^|;)\\s*DROP TABLE(?:\\s+IF EXISTS)?\\s+(\\S+)\\s*;");
  private final Pattern[] renamePatterns = new Pattern[]{
      Pattern.compile("(?i)(?:^|;)\\s*RENAME(?:\\s+TABLE)?\\s+(\\S+)\\s+TO\\s+(\\S+)\\s*;"),
      Pattern.compile("(?i)(?:^|;)\\s*ALTER\\s+TABLE(?:\\s+IF EXISTS)?\\s+(\\S+)\\s+RENAME\\s+TO\\s+(\\S+)\\s*;"),
      Pattern.compile("(?i)(?:^|;)\\s*Exec\\s+sp_rename\\s+(\\S+)\\s*,\\s*(\\S+)\\s*;")
  };


  @Override
  public void findTablesInScript(String scriptPath, String dbType, Set<String> tableList) throws HiveMetaException {
    if (!(new File(scriptPath)).exists()) {
      throw new HiveMetaException(scriptPath + " does not exist. Potentially incorrect version in the metastore VERSION table");
    }

    //TODO: This is really fragile as SQL sytax allows multiple statements in a single line, or single statement splitted
    // to multiple lines... Not sure how useful is this validation, it will keep fail if the SQL statements are
    // not conform to the patterns below.
    try (BufferedReader reader = new BufferedReader(new FileReader(scriptPath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("--")) {
          continue;
        }
        line = line.replaceAll("( )+", " "); //suppress multi-spaces
        line = line.replace("(", " ");
        line = line.replace("IF NOT EXISTS ", "");
        line = line.replace("`", "");
        line = line.replace("'", "");
        line = line.replace("\"", "");


        Matcher matcher = createPattern.matcher(line);
        if (matcher.find()) {
          String table = getTableName(dbType, matcher.group(1));
          tableList.add(table);
          LOG.debug("Found table " + table + " in the schema");
        }

        matcher = dropPattern.matcher(line);
        if (matcher.find()) {
          String table = getTableName(dbType, matcher.group(1));
          tableList.remove(table);
          LOG.debug("Found table " + table + " in the schema");
        }

        for(Pattern pattern : renamePatterns) {
          matcher = pattern.matcher(line);
          if (matcher.find()) {
            String oldTable = getTableName(dbType, matcher.group(1));
            String newTable = getTableName(dbType, matcher.group(2));
            tableList.remove(oldTable);
            tableList.add(newTable);
            LOG.debug("Found table rename from " + oldTable + " to " + newTable + " in the schema");
            break;
          }
        }
      }
    } catch (IOException ex){
      throw new HiveMetaException(ex.getMessage());
    }
  }

  private String getTableName(String dbType, String table) {
    if (dbType.equals("derby")) {
      table = table.replace("APP.", "");
    }
    return table.toLowerCase();
  }

}
