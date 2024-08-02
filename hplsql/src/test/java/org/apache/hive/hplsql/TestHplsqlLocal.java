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

package org.apache.hive.hplsql;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for HPL/SQL (no Hive connection required)
 */
public class TestHplsqlLocal {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();

  @Test
  public void testAdd() throws Exception {
    run("add");
  }

  @Test
  public void testAssign() throws Exception {
    run("assign");
  }

  @Test
  public void testBool() throws Exception {
    run("bool");
  }
  
  @Test
  public void testBoolExpr() throws Exception {
    run("bool_expr");
  }

  @Test
  public void testBreak() throws Exception {
    run("break");
  }

  @Test
  public void testCase() throws Exception {
    run("case");
  }

  @Test
  public void testCast() throws Exception {
    run("cast");
  }

  @Test
  public void testCast2() throws Exception {
    run("cast2");
  }

  @Test
  public void testChar() throws Exception {
    run("char");
  }

  @Test
  public void testCoalesce() throws Exception {
    run("coalesce");
  }

  @Test
  public void testConcat() throws Exception {
    run("concat");
  }

  @Test
  public void testCreateFunction() throws Exception {
    run("create_function");
  }

  @Test
  public void testCreateFunction2() throws Exception {
    run("create_function2");
  }
  
  @Test
  public void testCreateFunction3() throws Exception {
    run("create_function3");
  }
  
  @Test
  public void testCreateFunction4() throws Exception {
    run("create_function4");
  }
  
  @Test
  public void testCreateFunction5() throws Exception {
    run("create_function5");
  }

  @Test
  public void testCreatePackage() throws Exception {
    run("create_package");
  }
  
  @Test
  public void testCreatePackage2() throws Exception {
    run("create_package2");
  }
  
  @Test
  public void testCreatePackage3() throws Exception {
    run("create_package3");
  }

  @Test
  public void testDropPackage() throws Exception {
    run("drop_package");
  }

  @Test
  public void testDropProcedure() throws Exception {
    run("drop_proc");
  }

  @Test
  public void testCreateProcedure() throws Exception {
    run("create_procedure");
  }

  @Test
  public void testCreateProcedure2() throws Exception {
    run("create_procedure2");
  }

  @Test
  public void testCreateProcedure3() throws Exception {
    run("create_procedure3");
  }

  @Test
  public void testCreateProcedure4() throws Exception {
    run("create_procedure4");
  }
  
  @Test
  public void testCreateProcedureNoParams() throws Exception {
    run("create_procedure_no_params");
  }

  @Test
  public void testDatatypes() throws Exception {
    run("datatypes");
  }

  @Test
  public void testTableType() throws Exception {
    run("table_type");
  }

  @Test
  public void testDate() throws Exception {
    run("date");
  }

  @Test
  public void testDbmsOutput() throws Exception {
    run("dbms_output");
  }

  @Test
  public void testDeclare() throws Exception {
    run("declare");
  }

  @Test
  public void testDeclare2() throws Exception {
    run("declare2");
  }
  
  @Test
  public void testDeclare3() throws Exception {
    run("declare3");
  }

  @Test
  public void testDeclare4() throws Exception {
    run("declare4");
  }
  
  @Test
  public void testDeclareCondition() throws Exception {
    run("declare_condition");
  }

  @Test
  public void testDeclareCondition2() throws Exception {
    run("declare_condition2");
  }

  @Test
  public void testDecode() throws Exception {
    run("decode");
  }

  @Test
  public void testEqual() throws Exception {
    run("equal");
  }

  @Test
  public void testException() throws Exception {
    run("exception");
  }
  
  @Test
  public void testExceptionDivideByZero() throws Exception {
    run("exception_divide_by_zero");
  }

  @Test
  public void testExit() throws Exception {
    run("exit");
  }

  @Test
  public void testExpr() throws Exception {
    run("expr");
  }

  @Test
  public void testFloat() throws Exception {
    run("float");
  }
  
  @Test
  public void testForRange() throws Exception {
    run("for_range");
  }

  @Test
  public void testIf() throws Exception {
    run("if");
  }
  
  @Test
  public void testIf2() throws Exception {
    run("if2");
  }
  
  @Test
  public void testIf3Bteq() throws Exception {
    run("if3_bteq");
  }

  @Test
  public void testInclude() throws Exception {
    run("include");
  }

  @Test
  public void testInstr() throws Exception {
    run("instr");
  }

  @Test
  public void testInterval() throws Exception {
    run("interval");
  }

  @Test
  public void testLang() throws Exception {
    run("lang");
  }

  @Test
  public void testLeave() throws Exception {
    run("leave");
  }

  @Test
  public void testLength() throws Exception {
    run("length");
  }

  @Test
  public void testLen() throws Exception {
    run("len");
  }

  @Test
  public void testLower() throws Exception {
    run("lower");
  }

  @Test
  public void testMultDiv() throws Exception {
    run("mult_div");
  }

  @Test
  public void testNvl() throws Exception {
    run("nvl");
  }

  @Test
  public void testNvl2() throws Exception {
    run("nvl2");
  }

  @Test
  public void testPrint() throws Exception {
    run("print");
  }

  @Test
  public void testReplace() throws Exception {
    run("replace");
  }
  
  @Test
  public void testReturn() throws Exception {
    run("return");
  }

  @Test
  public void testSetError() throws Exception {
    run("seterror");
  }

  @Test
  public void testSub() throws Exception {
    run("sub");
  }

  @Test
  public void testSubstring() throws Exception {
    run("substring");
  }

  @Test
  public void testSubstr() throws Exception {
    run("substr");
  }

  @Test
  public void testTimestampIso() throws Exception {
    run("timestamp_iso");
  }

  @Test
  public void testTimestamp() throws Exception {
    run("timestamp");
  }

  @Test
  public void testToChar() throws Exception {
    run("to_char");
  }

  @Test
  public void testToTimestamp() throws Exception {
    run("to_timestamp");
  }

  @Test
  public void testTrim() throws Exception {
    run("trim");
  }

  @Test
  public void testTwoPipes() throws Exception {
    run("twopipes");
  }

  @Test
  public void testUpper() throws Exception {
    run("upper");
  }

  @Test
  public void testValuesInto() throws Exception {
    run("values_into");
  }
  
  @Test
  public void testVarScope() throws Exception {
    run("var_scope");
  }
  
  @Test
  public void testVarScope2() throws Exception {
    run("var_scope2");
  }

  @Test
  public void testWhile() throws Exception {
    run("while");
  }

  @Test
  public void testArity() throws Exception {
    run("arity");
  }

  @Test
  public void testArity2() throws Exception {
    run("arity2");
  }

  @Test
  public void testTypeCheck() throws Exception {
    run("type_check");
  }

  @Test
  public void testUndefFunc() throws Exception {
    run("undef_func");
  }

  @Test
  public void testUndefVar() throws Exception {
    run("undef_var");
  }

  @Test
  public void testNull() throws Exception {
    run("null");
  }

  @Test
  public void testFuncNoReturn() throws Exception {
    run("func_no_return");
  }

  @Test
  public void testInvalidSyntax() throws Exception {
    run("invalid_syntax");
  }

  @Test
  public void testPrecedence() throws Exception {
    run("preced");
  }

  @Test
  public void testConversion() throws Exception {
    run("conversion");
  }

  /**
   * Run a test file
   */
  void run(String testFile) throws Exception {
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
    Exec exec = new Exec();
    String[] args = { "-f", "src/test/queries/local/" + testFile + ".sql", "-trace" };
    exec.run(args);
    String sout = getTestOutput(out.toString());
    String serr = getTestOutput(err.toString());
    String output = (sout + (serr.isEmpty() ? "" : serr));
    FileUtils.writeStringToFile(new java.io.File("target/tmp/log/" + testFile + ".out.txt"), output);
    String t = FileUtils.readFileToString(new java.io.File("src/test/results/local/" + testFile + ".out.txt"), "utf-8");
    System.setOut(null);
    Assert.assertEquals(t, output);
  }

  /**
   * Get test output
   */
  String getTestOutput(String s) throws Exception {
    StringBuilder sb = new StringBuilder();
    BufferedReader reader = new BufferedReader(new StringReader(s));
    String line = null;
    while ((line = reader.readLine()) != null) {
      if (!line.startsWith("log4j:")
              && !line.contains("INFO Log4j")
              && !line.startsWith("SLF4J")
              && !line.contains(" StatusLogger ")
              && !line.contains("Configuration file: ")
              && !line.contains("Parser tree: ")) {
        sb.append(line);
        sb.append("\n");
      }
    }
    return sb.toString();
  }
}
