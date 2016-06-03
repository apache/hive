/**
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

package org.apache.hive.hplsql;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for HPL/SQL that generate SQL but do not run them (Hive connection no required)
 */
public class TestHplsqlOffline {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();

  @Test
  public void testCreateTableDb2() throws Exception {
    run("create_table_db2");
  }
  
  @Test
  public void testCreateTableMssql() throws Exception {
    run("create_table_mssql");
  }
  
  @Test
  public void testCreateTableMssql2() throws Exception {
    run("create_table_mssql2");
  }
  
  @Test
  public void testCreateTableMysql() throws Exception {
    run("create_table_mysql");
  }

  @Test
  public void testCreateTableOra() throws Exception {
    run("create_table_ora");
  }
  
  @Test
  public void testCreateTableOra2() throws Exception {
    run("create_table_ora2");
  }
  
  @Test
  public void testCreateTablePg() throws Exception {
    run("create_table_pg");
  }
  
  @Test
  public void testCreateTableTd() throws Exception {
    run("create_table_td");
  }
  
  @Test
  public void testDeleteAll() throws Exception {
    run("delete_all");
  }
  
  @Test
  public void testInsertMysql() throws Exception {
    run("insert_mysql");
  }

  @Test
  public void testSelect() throws Exception {
    run("select");
  }
  
  @Test
  public void testSelectDb2() throws Exception {
    run("select_db2");
  }
  
  @Test
  public void testSelectTeradata() throws Exception {
    run("select_teradata");
  }
  
  @Test
  public void testUpdate() throws Exception {
    run("update");
  }

  /**
   * Run a test file
   */
  void run(String testFile) throws Exception {
    System.setOut(new PrintStream(out));
    Exec exec = new Exec();
    String[] args = { "-f", "src/test/queries/offline/" + testFile + ".sql", "-trace", "-offline" };
    exec.run(args);
    String s = getTestOutput(out.toString()).trim();
    FileUtils.writeStringToFile(new java.io.File("target/tmp/log/" + testFile + ".out.txt"), s);
    String t = FileUtils.readFileToString(new java.io.File("src/test/results/offline/" + testFile + ".out.txt"), "utf-8").trim();
    System.setOut(null);
    Assert.assertEquals(s, t);
  }

  /**
   * Get test output
   */
  String getTestOutput(String s) throws Exception {
    StringBuilder sb = new StringBuilder();
    BufferedReader reader = new BufferedReader(new StringReader(s));
    String line = null;
    while ((line = reader.readLine()) != null) {
      if (!line.startsWith("log4j:") && !line.contains("INFO Log4j")) {
        sb.append(line);
        sb.append("\n");
      }
    }
    return sb.toString();
  }
}
