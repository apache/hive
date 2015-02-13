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
package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMacro;
import org.junit.Before;
import org.junit.Test;

public class TestMacroSemanticAnalyzer {

  private ParseDriver parseDriver;
  private MacroSemanticAnalyzer analyzer;
  private HiveConf conf;
  private Context context;

  @Before
  public void setup() throws Exception {
    conf = new HiveConf();
    SessionState.start(conf);
    context = new Context(conf);
    parseDriver = new ParseDriver();
    analyzer = new MacroSemanticAnalyzer(conf);
  }

  private ASTNode parse(String command) throws Exception {
    return ParseUtils.findRootNonNullToken(parseDriver.parse(command));
  }
  private void analyze(ASTNode ast) throws Exception {
    analyzer.analyze(ast, context);
    List<Task<? extends Serializable>> rootTasks = analyzer.getRootTasks();
    Assert.assertEquals(1, rootTasks.size());
    for(Task<? extends Serializable> task : rootTasks) {
      Assert.assertEquals(0, task.executeTask());
    }
  }
  @Test
  public void testDropMacroDoesNotExist() throws Exception {
    analyze(parse("DROP TEMPORARY MACRO SOME_MACRO"));
  }
  @Test
  public void testDropMacroExistsDoNotIgnoreErrors() throws Exception {
    conf.setBoolVar(ConfVars.DROPIGNORESNONEXISTENT, false);
    FunctionRegistry.registerTemporaryUDF("SOME_MACRO", GenericUDFMacro.class);
    analyze(parse("DROP TEMPORARY MACRO SOME_MACRO"));
  }
  @Test
  public void testDropMacro() throws Exception {
    FunctionRegistry.registerTemporaryUDF("SOME_MACRO", GenericUDFMacro.class);
    analyze(parse("DROP TEMPORARY MACRO SOME_MACRO"));
  }
  @Test(expected = SemanticException.class)
  public void testDropMacroNonExistent() throws Exception {
    conf.setBoolVar(ConfVars.DROPIGNORESNONEXISTENT, false);
    analyze(parse("DROP TEMPORARY MACRO SOME_MACRO"));
  }
  @Test
  public void testDropMacroNonExistentWithIfExists() throws Exception {
    analyze(parse("DROP TEMPORARY MACRO IF EXISTS SOME_MACRO"));
  }
  @Test
  public void testDropMacroNonExistentWithIfExistsDoNotIgnoreNonExistent() throws Exception {
    conf.setBoolVar(ConfVars.DROPIGNORESNONEXISTENT, false);
    analyze(parse("DROP TEMPORARY MACRO IF EXISTS SOME_MACRO"));
  }
  @Test
  public void testZeroInputParamters() throws Exception {
    analyze(parse("CREATE TEMPORARY MACRO FIXED_NUMBER() 1"));
  }
  @Test
  public void testOneInputParamters() throws Exception {
    analyze(parse("CREATE TEMPORARY MACRO SIGMOID (x DOUBLE) 1.0 / (1.0 + EXP(-x))"));
  }
  @Test
  public void testTwoInputParamters() throws Exception {
    analyze(parse("CREATE TEMPORARY MACRO DUMB_ADD (x INT, y INT) x + y"));
  }
  @Test
  public void testThreeInputParamters() throws Exception {
    analyze(parse("CREATE TEMPORARY MACRO DUMB_ADD (x INT, y INT, z INT) x + y + z"));
  }
  @Test(expected = ParseException.class)
  public void testCannotUseReservedWordAsName() throws Exception {
    parse("CREATE TEMPORARY MACRO DOUBLE (x DOUBLE) 1.0 / (1.0 + EXP(-x))");
  }
  @Test(expected = ParseException.class)
  public void testNoBody() throws Exception {
    parse("CREATE TEMPORARY MACRO DUMB_MACRO()");
  }
  @Test(expected = SemanticException.class)
  public void testUnknownInputParameter() throws Exception {
    analyze(parse("CREATE TEMPORARY MACRO BAD_MACRO (x INT, y INT) x + y + z"));
  }
  @Test(expected = SemanticException.class)
  public void testOneUnusedParameterName() throws Exception {
    analyze(parse("CREATE TEMPORARY MACRO BAD_MACRO (x INT, y INT) x"));
  }
  @Test(expected = SemanticException.class)
  public void testTwoUnusedParameterNames() throws Exception {
    analyze(parse("CREATE TEMPORARY MACRO BAD_MACRO (x INT, y INT, z INT) x"));
  }
  @Test(expected = SemanticException.class)
  public void testTwoDuplicateParameterNames() throws Exception {
    analyze(parse("CREATE TEMPORARY MACRO BAD_MACRO (x INT, x INT) x + x"));
  }
  @Test(expected = SemanticException.class)
  public void testThreeDuplicateParameters() throws Exception {
    analyze(parse("CREATE TEMPORARY MACRO BAD_MACRO (x INT, x INT, x INT) x + x + x"));
  }
}