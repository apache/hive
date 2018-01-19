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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.hive.hplsql.HplsqlParser.Package_spec_itemContext;
import org.apache.hive.hplsql.HplsqlParser.Package_body_itemContext;
import org.apache.hive.hplsql.HplsqlParser.Create_function_stmtContext;
import org.apache.hive.hplsql.HplsqlParser.Create_procedure_stmtContext;
import org.apache.hive.hplsql.functions.Function;

/**
 * Program package
 */
public class Package {
  
  String name;
  ArrayList<Var> vars = new ArrayList<Var>();
  ArrayList<String> publicVars = new ArrayList<String>();
  ArrayList<String> publicFuncs = new ArrayList<String>();
  ArrayList<String> publicProcs = new ArrayList<String>();
  
  HashMap<String, Create_function_stmtContext> func = new HashMap<String, Create_function_stmtContext>();
  HashMap<String, Create_procedure_stmtContext> proc = new HashMap<String, Create_procedure_stmtContext>();
    
  boolean allMembersPublic = false;
    
  Exec exec;
  Function function;
  boolean trace = false;
  
  Package(String name, Exec exec) {
    this.name = name;
    this.exec = exec;
    this.function = new Function(exec);
    this.trace = exec.getTrace();
  }
  
  /**
   * Add a local variable
   */
  void addVariable(Var var) {
    vars.add(var);
  }
  
  /**
   * Find the variable by name
   */
  Var findVariable(String name) {
    for (Var var : vars) {
      if (name.equalsIgnoreCase(var.getName())) {
        return var;
      }
    }
    return null;
  }
  
  /**
   * Create the package specification
   */
  void createSpecification(HplsqlParser.Create_package_stmtContext ctx) {
    int cnt = ctx.package_spec().package_spec_item().size();
    for (int i = 0; i < cnt; i++) {
      Package_spec_itemContext c = ctx.package_spec().package_spec_item(i);
      if (c.declare_stmt_item() != null) {
        visit(c);
      }
      else if (c.T_FUNCTION() != null) {
        publicFuncs.add(c.ident().getText().toUpperCase());
      }
      else if (c.T_PROC() != null || c.T_PROCEDURE() != null) {
        publicProcs.add(c.ident().getText().toUpperCase());
      }
    }
  } 
  
  /**
   * Create the package body
   */
  void createBody(HplsqlParser.Create_package_body_stmtContext ctx) {
    int cnt = ctx.package_body().package_body_item().size();
    for (int i = 0; i < cnt; i++) {
      Package_body_itemContext c = ctx.package_body().package_body_item(i);
      if (c.declare_stmt_item() != null) {
        visit(c);
      }
      else if (c.create_function_stmt() != null) {
        func.put(c.create_function_stmt().ident().getText().toUpperCase(), c.create_function_stmt());
      }
      else if (c.create_procedure_stmt() != null) {
        proc.put(c.create_procedure_stmt().ident(0).getText().toUpperCase(), c.create_procedure_stmt());
      }
    }    
  } 
  
  /**
   * Execute function
   */
  public boolean execFunc(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    Create_function_stmtContext f = func.get(name.toUpperCase());
    if (f == null) {
      return execProc(name, ctx, false /*trace error if not exists*/);
    }
    if (trace) {
      trace(ctx, "EXEC PACKAGE FUNCTION " + this.name + "." + name);
    }
    ArrayList<Var> actualParams = function.getActualCallParameters(ctx);
    exec.enterScope(Scope.Type.ROUTINE, this);
    function.setCallParameters(ctx, actualParams, f.create_routine_params(), null);    
    visit(f.single_block_stmt());
    exec.leaveScope(); 
    return true;
  }
  
  /**
   * Execute procedure
   */
  public boolean execProc(String name, HplsqlParser.Expr_func_paramsContext ctx, boolean traceNotExists) {
    Create_procedure_stmtContext p = proc.get(name.toUpperCase());
    if (p == null) {
      if (trace && traceNotExists) {
        trace(ctx, "Package procedure not found: " + this.name + "." + name);
      }
      return false;        
    }
    if (trace) {
      trace(ctx, "EXEC PACKAGE PROCEDURE " + this.name + "." + name);
    }
    ArrayList<Var> actualParams = function.getActualCallParameters(ctx);
    HashMap<String, Var> out = new HashMap<String, Var>();
    exec.enterScope(Scope.Type.ROUTINE, this);
    exec.callStackPush(name);
    if (p.declare_block_inplace() != null) {
      visit(p.declare_block_inplace());
    }
    if (p.create_routine_params() != null) {
      function.setCallParameters(ctx, actualParams, p.create_routine_params(), out);
    }
    visit(p.proc_block());
    exec.callStackPop();
    exec.leaveScope();       
    for (Map.Entry<String, Var> i : out.entrySet()) {      // Set OUT parameters
      exec.setVariable(i.getKey(), i.getValue());
    }
    return true;
  }
  
  /**
   * Set whether all members are public (when package specification is missed) or not 
   */
  void setAllMembersPublic(boolean value) {
    allMembersPublic = value;
  }
  
  /**
   * Execute rules
   */
  Integer visit(ParserRuleContext ctx) {
    return exec.visit(ctx);  
  } 
  
  /**
   * Execute children rules
   */
  Integer visitChildren(ParserRuleContext ctx) {
    return exec.visitChildren(ctx);  
  }  
  
  /**
   * Trace information
   */
  public void trace(ParserRuleContext ctx, String message) {
    if (trace) {
      exec.trace(ctx, message);
    }
  }
}
