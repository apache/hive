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

package org.apache.hive.hplsql.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.hive.hplsql.ArityException;
import org.apache.hive.hplsql.Exec;
import org.apache.hive.hplsql.HplsqlParser;
import org.apache.hive.hplsql.Scope;
import org.apache.hive.hplsql.Var;
import org.apache.hive.hplsql.objects.TableClass;

interface FuncCommand {
  void run(HplsqlParser.Expr_func_paramsContext ctx);
}

interface FuncSpecCommand {
  void run(HplsqlParser.Expr_spec_funcContext ctx);
}

/**
 * HPL/SQL functions
 */
public class InMemoryFunctionRegistry implements FunctionRegistry {
  Exec exec;
  private BuiltinFunctions builtinFunctions;
  HashMap<String, HplsqlParser.Create_function_stmtContext> funcMap = new HashMap<>();
  HashMap<String, HplsqlParser.Create_procedure_stmtContext> procMap = new HashMap<>();
  boolean trace = false;
  
  public InMemoryFunctionRegistry(Exec e, BuiltinFunctions builtinFunctions) {
    this.exec = e;
    this.trace = exec.getTrace();
    this.builtinFunctions = builtinFunctions;
  }

  @Override
  public boolean exists(String name) {
    return funcMap.containsKey(name) || procMap.containsKey(name);
  }

  @Override
  public void remove(String name) {
    funcMap.remove(name);
    procMap.remove(name);
  }

  @Override
  public boolean exec(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    if (builtinFunctions.exec(name, ctx)) {
      return true;
    }
    if (execFunction(name, ctx)) {
      return true;
    }
    return (procMap.get(name) != null && execProc(name, ctx));
  }

  /**
   * Execute a user-defined function
   */
  private boolean execFunction(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    HplsqlParser.Create_function_stmtContext userCtx = funcMap.get(name);
    if (userCtx == null) {
      return false;
    }
    if (trace) {
      trace(ctx, "EXEC FUNCTION " + name);
    }
    ArrayList<Var> actualParams = getActualCallParameters(ctx);
    exec.enterScope(Scope.Type.ROUTINE);
    setCallParameters(name, ctx, actualParams, userCtx.create_routine_params(), null, exec);
    if (userCtx.declare_block_inplace() != null) {
      visit(userCtx.declare_block_inplace());
    }
    visit(userCtx.single_block_stmt());
    exec.leaveScope(); 
    return true;
  }

  /**
   * Execute a stored procedure using CALL or EXEC statement passing parameters
   */
  private boolean execProc(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    if (trace) {
      trace(ctx == null ? null : ctx.getParent(), "EXEC PROCEDURE " + name);
    }
    HplsqlParser.Create_procedure_stmtContext procCtx = procMap.get(name);
    if (procCtx == null) {
      trace(ctx.getParent(), "Procedure not found");
      return false;
    }    
    ArrayList<Var> actualParams = getActualCallParameters(ctx);
    HashMap<String, Var> out = new HashMap<>();
    exec.enterScope(Scope.Type.ROUTINE);
    exec.callStackPush(name);
    if (procCtx.declare_block_inplace() != null) {
      visit(procCtx.declare_block_inplace());
    }
    if (procCtx.create_routine_params() != null) {
      setCallParameters(name, ctx, actualParams, procCtx.create_routine_params(), out, exec);
    }
    visit(procCtx.proc_block());
    exec.callStackPop();
    exec.leaveScope();       
    for (Map.Entry<String, Var> i : out.entrySet()) {      // Set OUT parameters
      exec.setVariable(i.getKey(), i.getValue());
    }
    return true;
  }

  /**
   * Set parameters for user-defined function call
   */
  public static void setCallParameters(String procName, HplsqlParser.Expr_func_paramsContext actual,
      ArrayList<Var> actualValues, HplsqlParser.Create_routine_paramsContext formal, HashMap<String, Var> out,
      Exec exec) {
    // if it is a non-parameter function then just return.
    if (actual == null && formal == null) {
      return;
    }
    int actualCnt = (actualValues == null) ? 0 : actualValues.size();
    List<HplsqlParser.Create_routine_param_itemContext> routineParamItem = formal.create_routine_param_item();
    int formalCnt = routineParamItem.size();
    ParserRuleContext ruleContext = (actual == null) ? null : actual.getParent();
    if (actualCnt > formalCnt) {
      throw new ArityException(ruleContext, procName, formalCnt, actualCnt);
    }
    Map<String, Integer> defaultParamNamesVsIndexes = new HashMap<>();
    if (actualCnt != formalCnt) {
      populateDefaultParamDetails(routineParamItem, defaultParamNamesVsIndexes);
    }

    // set the passed params
    for (int i = 0; i < actualCnt; i++) {
      HplsqlParser.ExprContext exprContext = actual.func_param(i).expr();
      HplsqlParser.Create_routine_param_itemContext paramItemContext = getCallParameter(actual, formal, i);
      Var value = actualValues.get(i);
      // for any default param value is passed then remove it from default param list
      defaultParamNamesVsIndexes.remove(paramItemContext.ident().getText());
      setCallParameter(actual, out, exec, exprContext, paramItemContext, value);
    }
    // set the remaining default params
    for (int index : defaultParamNamesVsIndexes.values()) {
      HplsqlParser.Create_routine_param_itemContext paramItemContext = formal.create_routine_param_item().get(index);
      HplsqlParser.ExprContext exprContext = paramItemContext.dtype_default().expr();
      Var value = exec.evalPop(paramItemContext.dtype_default().expr());
      setCallParameter(actual, out, exec, exprContext, paramItemContext, value);
    }
    // if actual param count + remaining default param count is lesser than formal param count then throw exception as some params are missing
    if ((actualCnt + defaultParamNamesVsIndexes.size()) != formalCnt) {
      throw new ArityException(ruleContext, procName, formalCnt, actualCnt);
    }
  }

  private static void populateDefaultParamDetails(List<HplsqlParser.Create_routine_param_itemContext> routineParamItem,
      Map<String, Integer> defaultParamNamesVsIndexes) {
    int formalCnt = routineParamItem.size();
    for (int i = 0; i < formalCnt; i++) {
      HplsqlParser.Create_routine_param_itemContext routineParamItemContext = routineParamItem.get(i);
      if (routineParamItemContext.dtype_default() != null) {
        defaultParamNamesVsIndexes.put(routineParamItemContext.ident().getText(), i);
      }
    }
  }

  private static void setCallParameter(HplsqlParser.Expr_func_paramsContext actual, HashMap<String, Var> out, Exec exec,
      HplsqlParser.ExprContext a, HplsqlParser.Create_routine_param_itemContext p, Var value) {
    String name = p.ident().getText();
    String type = p.dtype().getText();
    String len = null;
    String scale = null;
    if (p.dtype_len() != null) {
      len = p.dtype_len().L_INT(0).getText();
      if (p.dtype_len().L_INT(1) != null) {
        scale = p.dtype_len().L_INT(1).getText();
      }
    }
    Var variable = setCallParameter(name, type, len, scale, value, exec);
    exec.trace(actual, "SET PARAM " + name + " = " + variable.toString());
    if (out != null && a.expr_atom() != null && a.expr_atom()
        .qident() != null && (p.T_OUT() != null || p.T_INOUT() != null)) {
      String actualName = a.expr_atom().qident().getText();
      if (actualName != null) {
        out.put(actualName, variable);
      }
    }
  }

  /**
   * Create a function or procedure parameter and set its value
   */
  static Var setCallParameter(String name, String typeName, String len, String scale, Var value, Exec exec) {
    TableClass hplClass = exec.getType(typeName);
    Var var =new Var(name, hplClass == null ? typeName : Var.Type.HPL_OBJECT.name(), len, scale, null);
    if (hplClass != null) {
      var.setValue(hplClass.newInstance());
    }
    var.cast(value);
    exec.addVariable(var);
    return var;
  }
  
  /**
   * Get call parameter definition by name (if specified) or position
   */
  static HplsqlParser.Create_routine_param_itemContext getCallParameter(HplsqlParser.Expr_func_paramsContext actual,
      HplsqlParser.Create_routine_paramsContext formal, int pos) {
    String named ;
    int out_pos = pos;
    if (actual.func_param(pos).ident() != null) {
      named = actual.func_param(pos).ident().getText();
      int cnt = formal.create_routine_param_item().size();
      for (int i = 0; i < cnt; i++) {
        if (named.equalsIgnoreCase(formal.create_routine_param_item(i).ident().getText())) {
          out_pos = i;
          break;
        }
      }
    }
    return formal.create_routine_param_item(out_pos);
  }  
  
  /**
   * Evaluate actual call parameters
   */
  public ArrayList<Var> getActualCallParameters(HplsqlParser.Expr_func_paramsContext actual) {
    if (actual == null || actual.func_param() == null) {
      return null;
    }
    int cnt = actual.func_param().size();
    ArrayList<Var> values = new ArrayList<>(cnt);
    for (int i = 0; i < cnt; i++) {
      values.add(evalPop(actual.func_param(i).expr()));
    }
    return values;
  }

  @Override
  public void addUserFunction(HplsqlParser.Create_function_stmtContext ctx) {
    String name = ctx.ident().getText().toUpperCase();
    if (builtinFunctions.exists(name)) {
      exec.info(ctx, name + " is a built-in function which cannot be redefined.");
      return;
    }
    if (trace) {
      trace(ctx, "CREATE FUNCTION " + name);
    }
    funcMap.put(name.toUpperCase(), ctx);
  }

  @Override
  public void addUserProcedure(HplsqlParser.Create_procedure_stmtContext ctx) {
    String name = ctx.ident(0).getText().toUpperCase();
    if (builtinFunctions.exists(name)) {
      exec.info(ctx, name + " is a built-in function which cannot be redefined.");
      return;
    }
    if (trace) {
      trace(ctx, "CREATE PROCEDURE " + name);
    }
    procMap.put(name.toUpperCase(), ctx);
  }

  /**
   * Evaluate the expression and pop value from the stack
   */
  private Var evalPop(ParserRuleContext ctx) {
    exec.visit(ctx);
    return exec.stackPop();  
  }

  /**
   * Execute rules
   */
  private Integer visit(ParserRuleContext ctx) {
    return exec.visit(ctx);  
  }

  private void trace(ParserRuleContext ctx, String message) {
    if (trace) {
      exec.trace(ctx, message);
    }
  }
}
