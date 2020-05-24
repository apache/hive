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

import java.sql.ResultSet;
import java.sql.Date;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.hive.hplsql.*;

interface FuncCommand {
  void run(HplsqlParser.Expr_func_paramsContext ctx);
}

interface FuncSpecCommand {
  void run(HplsqlParser.Expr_spec_funcContext ctx);
}

/**
 * HPL/SQL functions
 */
public class Function {
  Exec exec;
  HashMap<String, FuncCommand> map = new HashMap<String, FuncCommand>();  
  HashMap<String, FuncSpecCommand> specMap = new HashMap<String, FuncSpecCommand>();
  HashMap<String, FuncSpecCommand> specSqlMap = new HashMap<String, FuncSpecCommand>();
  HashMap<String, HplsqlParser.Create_function_stmtContext> userMap = new HashMap<String, HplsqlParser.Create_function_stmtContext>();
  HashMap<String, HplsqlParser.Create_procedure_stmtContext> procMap = new HashMap<String, HplsqlParser.Create_procedure_stmtContext>();
  boolean trace = false; 
  
  public Function(Exec e) {
    exec = e;  
    trace = exec.getTrace();
  }
  
  /** 
   * Register functions
   */
  public void register(Function f) {    
  }
  
  /**
   * Execute a function
   */
  public void exec(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    if (execUser(name, ctx)) {
      return;
    }
    else if (isProc(name) && execProc(name, ctx, null)) {
      return;
    }
    if (name.indexOf(".") != -1) {               // Name can be qualified and spaces are allowed between parts
      String[] parts = name.split("\\.");
      StringBuilder str = new StringBuilder();
      for (int i = 0; i < parts.length; i++) {
        if (i > 0) {
          str.append(".");
        }
        str.append(parts[i].trim());        
      }
      name = str.toString();      
    } 
    if (trace && ctx != null && ctx.parent != null && ctx.parent.parent instanceof HplsqlParser.Expr_stmtContext) {
      trace(ctx, "FUNC " + name);      
    }
    FuncCommand func = map.get(name.toUpperCase());    
    if (func != null) {
      func.run(ctx);
    }    
    else {
      info(ctx, "Function not found: " + name);
      evalNull();
    }
  }
  
  /**
   * User-defined function in a SQL query
   */
  public void execSql(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    if (execUserSql(ctx, name)) {
      return;
    }
    StringBuilder sql = new StringBuilder();
    sql.append(name);
    sql.append("(");
    if (ctx != null) {
      int cnt = ctx.func_param().size();
      for (int i = 0; i < cnt; i++) {
        sql.append(evalPop(ctx.func_param(i).expr()));
        if (i + 1 < cnt) {
          sql.append(", ");
        }
      }
    }
    sql.append(")");
    exec.stackPush(sql);
  }
  
  /**
   * Aggregate or window function in a SQL query
   */
  public void execAggWindowSql(HplsqlParser.Expr_agg_window_funcContext ctx) {
    exec.stackPush(exec.getFormattedText(ctx));
  }
  
  /**
   * Execute a user-defined function
   */
  public boolean execUser(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    HplsqlParser.Create_function_stmtContext userCtx = userMap.get(name.toUpperCase());
    if (userCtx == null) {
      return false;
    }
    if (trace) {
      trace(ctx, "EXEC FUNCTION " + name);
    }
    ArrayList<Var> actualParams = getActualCallParameters(ctx);
    exec.enterScope(Scope.Type.ROUTINE);
    setCallParameters(ctx, actualParams, userCtx.create_routine_params(), null);
    if (userCtx.declare_block_inplace() != null) {
      visit(userCtx.declare_block_inplace());
    }
    visit(userCtx.single_block_stmt());
    exec.leaveScope(); 
    return true;
  }
  
  /**
   * Execute a HPL/SQL user-defined function in a query 
   */
  public boolean execUserSql(HplsqlParser.Expr_func_paramsContext ctx, String name) {
    HplsqlParser.Create_function_stmtContext userCtx = userMap.get(name.toUpperCase());
    if (userCtx == null) {
      return false;
    }
    StringBuilder sql = new StringBuilder();
    sql.append("hplsql('");
    sql.append(name);
    sql.append("(");
    int cnt = ctx.func_param().size();
    for (int i = 0; i < cnt; i++) {
      sql.append(":" + (i + 1));
      if (i + 1 < cnt) {
        sql.append(", ");
      }
    }
    sql.append(")'");
    if (cnt > 0) {
      sql.append(", ");
    }
    for (int i = 0; i < cnt; i++) {
      sql.append(evalPop(ctx.func_param(i).expr()));
      if (i + 1 < cnt) {
        sql.append(", ");
      }
    }
    sql.append(")");
    exec.stackPush(sql);
    exec.registerUdf();
    return true;
  }
  
  /**
   * Execute a stored procedure as the entry point of the script (defined by -main option)
   */
  public boolean execProc(String name) {
    if (trace) {
      trace("EXEC PROCEDURE " + name);
    }
    HplsqlParser.Create_procedure_stmtContext procCtx = procMap.get(name.toUpperCase());    
    if (procCtx == null) {
      trace("Procedure not found");
      return false;
    }    
    exec.enterScope(Scope.Type.ROUTINE);
    exec.callStackPush(name);
    if (procCtx.create_routine_params() != null) {
      setCallParameters(procCtx.create_routine_params());
    }
    visit(procCtx.proc_block());
    exec.callStackPop();
    exec.leaveScope();       
    return true;
  }
  
  /**
   * Check if the stored procedure with the specified name is defined
   */
  public boolean isProc(String name) {
    if (procMap.get(name.toUpperCase()) != null) {
      return true;
    }
    return false;
  }
  
  /**
   * Execute a stored procedure using CALL or EXEC statement passing parameters
   */
  public boolean execProc(String name, HplsqlParser.Expr_func_paramsContext ctx, ParserRuleContext callCtx) {
    if (trace) {
      trace(callCtx, "EXEC PROCEDURE " + name);
    }
    HplsqlParser.Create_procedure_stmtContext procCtx = procMap.get(name.toUpperCase());    
    if (procCtx == null) {
      trace(callCtx, "Procedure not found");
      return false;
    }    
    ArrayList<Var> actualParams = getActualCallParameters(ctx);
    HashMap<String, Var> out = new HashMap<String, Var>();
    exec.enterScope(Scope.Type.ROUTINE);
    exec.callStackPush(name);
    if (procCtx.declare_block_inplace() != null) {
      visit(procCtx.declare_block_inplace());
    }
    if (procCtx.create_routine_params() != null) {
      setCallParameters(ctx, actualParams, procCtx.create_routine_params(), out);
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
  public void setCallParameters(HplsqlParser.Expr_func_paramsContext actual, ArrayList<Var> actualValues, 
                         HplsqlParser.Create_routine_paramsContext formal,
                         HashMap<String, Var> out) {
    if (actual == null || actual.func_param() == null || actualValues == null) {
      return;
    }
    int actualCnt = actualValues.size();
    int formalCnt = formal.create_routine_param_item().size();
    for (int i = 0; i < actualCnt; i++) {
      if (i >= formalCnt) {
        break;
      }
      HplsqlParser.ExprContext a = actual.func_param(i).expr(); 
      HplsqlParser.Create_routine_param_itemContext p = getCallParameter(actual, formal, i);
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
      Var var = setCallParameter(name, type, len, scale, actualValues.get(i));
      if (trace) {
        trace(actual, "SET PARAM " + name + " = " + var.toString());      
      } 
      if (out != null && a.expr_atom() != null && a.expr_atom().ident() != null &&
          (p.T_OUT() != null || p.T_INOUT() != null)) {
        String actualName = a.expr_atom().ident().getText();
        if (actualName != null) {
          out.put(actualName, var);  
        }         
      }
    }
  }
  
  /**
   * Set parameters for entry-point call (Main procedure defined by -main option)
   */
  void setCallParameters(HplsqlParser.Create_routine_paramsContext ctx) {
    int cnt = ctx.create_routine_param_item().size();
    for (int i = 0; i < cnt; i++) {
      HplsqlParser.Create_routine_param_itemContext p = ctx.create_routine_param_item(i);
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
      Var value = exec.findVariable(name);
      Var var = setCallParameter(name, type, len, scale, value);
      if (trace) {
        trace(ctx, "SET PARAM " + name + " = " + var.toString());      
      }      
    }
  }
  
  /**
   * Create a function or procedure parameter and set its value
   */
  Var setCallParameter(String name, String type, String len, String scale, Var value) {
    Var var = new Var(name, type, len, scale, null);
    var.cast(value);
    exec.addVariable(var);    
    return var;
  }
  
  /**
   * Get call parameter definition by name (if specified) or position
   */
  HplsqlParser.Create_routine_param_itemContext getCallParameter(HplsqlParser.Expr_func_paramsContext actual, 
      HplsqlParser.Create_routine_paramsContext formal, int pos) {
    String named = null;
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
    ArrayList<Var> values = new ArrayList<Var>(cnt);
    for (int i = 0; i < cnt; i++) {
      values.add(evalPop(actual.func_param(i).expr()));
    }
    return values;
  }
  
  /**
   * Add a user-defined function
   */
  public void addUserFunction(HplsqlParser.Create_function_stmtContext ctx) {
    String name = ctx.ident().getText();
    if (trace) {
      trace(ctx, "CREATE FUNCTION " + name);
    }
    userMap.put(name.toUpperCase(), ctx);
  }
  
  /**
   * Add a user-defined procedure
   */
  public void addUserProcedure(HplsqlParser.Create_procedure_stmtContext ctx) {
    String name = ctx.ident(0).getText();
    if (trace) {
      trace(ctx, "CREATE PROCEDURE " + name);
    }
    procMap.put(name.toUpperCase(), ctx);
  }
  
  /**
   * Get the number of parameters in function call
   */
  public int getParamCount(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx == null) {
      return 0;
    }
    return ctx.func_param().size();
  }
    
  /**
   * Execute a special function
   */
  public void specExec(HplsqlParser.Expr_spec_funcContext ctx) {
    String name = ctx.start.getText().toUpperCase();
    if (trace && ctx.parent.parent instanceof HplsqlParser.Expr_stmtContext) {
      trace(ctx, "FUNC " + name);      
    }
    FuncSpecCommand func = specMap.get(name);    
    if (func != null) {
      func.run(ctx);
    }
    else if(ctx.T_MAX_PART_STRING() != null) {
      execMaxPartString(ctx);
    } else if(ctx.T_MIN_PART_STRING() != null) {
      execMinPartString(ctx);
    } else if(ctx.T_MAX_PART_INT() != null) {
      execMaxPartInt(ctx);
    } else if(ctx.T_MIN_PART_INT() != null) {
      execMinPartInt(ctx);
    } else if(ctx.T_MAX_PART_DATE() != null) {
      execMaxPartDate(ctx);
    } else if(ctx.T_MIN_PART_DATE() != null) {
      execMinPartDate(ctx);
    } else if(ctx.T_PART_LOC() != null) {
      execPartLoc(ctx);
    } else {
      evalNull();
    }
  }
  
  /**
   * Execute a special function in executable SQL statement
   */
  public void specExecSql(HplsqlParser.Expr_spec_funcContext ctx) {
    String name = ctx.start.getText().toUpperCase();
    if (trace && ctx.parent.parent instanceof HplsqlParser.Expr_stmtContext) {
      trace(ctx, "FUNC " + name);      
    }
    FuncSpecCommand func = specSqlMap.get(name);    
    if (func != null) {
      func.run(ctx);
    }
    else {
      exec.stackPush(exec.getFormattedText(ctx));
    }
  }
  
  /**
   * Get the current date
   */
  public void execCurrentDate(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "CURRENT_DATE");
    }
    SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
    String s = f.format(Calendar.getInstance().getTime());
    exec.stackPush(new Var(Var.Type.DATE, Utils.toDate(s))); 
  }
  
  /**
   * Execute MAX_PART_STRING function
   */
  public void execMaxPartString(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MAX_PART_STRING");
    }
    execMinMaxPart(ctx, Var.Type.STRING, true /*max*/);
  }
  
  /**
   * Execute MIN_PART_STRING function
   */
  public void execMinPartString(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MIN_PART_STRING");
    }
    execMinMaxPart(ctx, Var.Type.STRING, false /*max*/);
  }

  /**
   * Execute MAX_PART_INT function
   */
  public void execMaxPartInt(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MAX_PART_INT");
    }
    execMinMaxPart(ctx, Var.Type.BIGINT, true /*max*/);
  }
  
  /**
   * Execute MIN_PART_INT function
   */
  public void execMinPartInt(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MIN_PART_INT");
    }
    execMinMaxPart(ctx, Var.Type.BIGINT, false /*max*/);
  }

  /**
   * Execute MAX_PART_DATE function
   */
  public void execMaxPartDate(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MAX_PART_DATE");
    }
    execMinMaxPart(ctx, Var.Type.DATE, true /*max*/);
  }
  
  /**
   * Execute MIN_PART_DATE function
   */
  public void execMinPartDate(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MIN_PART_DATE");
    }
    execMinMaxPart(ctx, Var.Type.DATE, false /*max*/);
  }
  
  /**
   * Execute MIN or MAX partition function
   */
  public void execMinMaxPart(HplsqlParser.Expr_spec_funcContext ctx, Var.Type type, boolean max) {
    String tabname = evalPop(ctx.expr(0)).toString();
    String sql = "SHOW PARTITIONS " + tabname;    
    String colname = null;    
    int colnum = -1;
    int exprnum = ctx.expr().size();    
    // Column name 
    if (ctx.expr(1) != null) {
      colname = evalPop(ctx.expr(1)).toString();
    } else {
      colnum = 0;
    }
    // Partition filter
    if (exprnum >= 4) {
      sql += " PARTITION (";
      int i = 2;
      while (i + 1 < exprnum) {
        String fcol = evalPop(ctx.expr(i)).toString();
        String fval = evalPop(ctx.expr(i+1)).toSqlString();
        if (i > 2) {
          sql += ", ";
        }
        sql += fcol + "=" + fval;        
        i += 2;
      }
      sql += ")";
    }
    if (trace) {
      trace(ctx, "Query: " + sql);
    }
    if (exec.getOffline()) {
      evalNull();
      return;
    }
    Query query = exec.executeQuery(ctx, sql, exec.conf.defaultConnection);
    if (query.error()) {
      evalNullClose(query, exec.conf.defaultConnection);
      return;
    }
    ResultSet rs = query.getResultSet();
    try {
      String resultString = null;
      Long resultInt = null;
      Date resultDate = null;      
      while (rs.next()) {
        String[] parts = rs.getString(1).split("/");
        // Find partition column by name
        if (colnum == -1) {
          for (int i = 0; i < parts.length; i++) {
            String[] name = parts[i].split("=");
            if (name[0].equalsIgnoreCase(colname)) {
              colnum = i;
              break;
            }
          }
          // No partition column with the specified name exists
          if (colnum == -1) {
            evalNullClose(query, exec.conf.defaultConnection);
            return;
          }
        }
        String[] pair = parts[colnum].split("=");
        if (type == Var.Type.STRING) {
          resultString = Utils.minMaxString(resultString, pair[1], max);          
        } 
        else if (type == Var.Type.BIGINT) {
          resultInt = Utils.minMaxInt(resultInt, pair[1], max);          
        } 
        else if (type == Var.Type.DATE) {
          resultDate = Utils.minMaxDate(resultDate, pair[1], max);
        }
      }
      if (resultString != null) {
        evalString(resultString);
      } 
      else if (resultInt != null) {
        evalInt(resultInt);
      } 
      else if (resultDate != null) {
        evalDate(resultDate);
      } 
      else {
        evalNull();
      }
    } catch (SQLException e) {}  
    exec.closeQuery(query, exec.conf.defaultConnection);
  }
  
  /**
   * Execute PART_LOC function
   */
  public void execPartLoc(HplsqlParser.Expr_spec_funcContext ctx) {
    String tabname = evalPop(ctx.expr(0)).toString();
    String sql = "DESCRIBE EXTENDED " + tabname;    
    int exprnum = ctx.expr().size();   
    boolean hostname = false;
    // Partition filter
    if (exprnum > 1) {
      sql += " PARTITION (";
      int i = 1;
      while (i + 1 < exprnum) {
        String col = evalPop(ctx.expr(i)).toString();
        String val = evalPop(ctx.expr(i+1)).toSqlString();
        if (i > 2) {
          sql += ", ";
        }
        sql += col + "=" + val;        
        i += 2;
      }
      sql += ")";
    }
    // With host name
    if (exprnum % 2 == 0 && evalPop(ctx.expr(exprnum - 1)).intValue() == 1) {
      hostname = true;
    }
    if (trace) {
      trace(ctx, "Query: " + sql);
    }
    if (exec.getOffline()) {
      evalNull();
      return;
    }
    Query query = exec.executeQuery(ctx, sql, exec.conf.defaultConnection);
    if (query.error()) {
      evalNullClose(query, exec.conf.defaultConnection);
      return;
    }
    String result = null;
    ResultSet rs = query.getResultSet();
    try {
      while (rs.next()) {
        if (rs.getString(1).startsWith("Detailed Partition Information")) {
          Matcher m = Pattern.compile(".*, location:(.*?),.*").matcher(rs.getString(2));
          if (m.find()) {
            result = m.group(1);
          }    
        }
      }
    } catch (SQLException e) {}  
    if (result != null) {
      // Remove the host name
      if (!hostname) {
        Matcher m = Pattern.compile(".*://.*?(/.*)").matcher(result); 
        if (m.find()) {
          result = m.group(1);
        }
      }
      evalString(result);
    }    
    else {
      evalNull();
    }
    exec.closeQuery(query, exec.conf.defaultConnection);
  }
  
  /**
   * Evaluate the expression and push the value to the stack
   */
  void eval(ParserRuleContext ctx) {
    exec.visit(ctx);
  }

  /**
   * Evaluate the expression to the specified variable
   */
  void evalVar(Var var) {
    exec.stackPush(var); 
  }

  /**
   * Evaluate the expression to NULL
   */
  void evalNull() {
    exec.stackPush(Var.Null); 
  }
  
  /**
   * Evaluate the expression to specified String value
   */
  void evalString(String string) {
    exec.stackPush(new Var(string)); 
  }
  
  void evalString(StringBuilder string) {
    evalString(string.toString()); 
  }

  /**
   * Evaluate the expression to specified Int value
   */
  void evalInt(Long i) {
    exec.stackPush(new Var(i)); 
  }
  
  void evalInt(int i) {
    evalInt(Long.valueOf(i));
  }
  
  /**
   * Evaluate the expression to specified Date value
   */
  void evalDate(Date date) {
    exec.stackPush(new Var(Var.Type.DATE, date)); 
  }
  
  /**
   * Evaluate the expression to NULL and close the query
   */
  void evalNullClose(Query query, String conn) {
    exec.stackPush(Var.Null); 
    exec.closeQuery(query, conn);
    if(trace) {
      query.printStackTrace();
    }
  }
  
  /**
   * Evaluate the expression and pop value from the stack
   */
  Var evalPop(ParserRuleContext ctx) {
    exec.visit(ctx);
    return exec.stackPop();  
  }
  
  Var evalPop(ParserRuleContext ctx, int value) {
    if (ctx != null) {
      return evalPop(ctx);
    }
    return new Var(Long.valueOf(value));
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
  
  public void trace(String message) {
    trace(null, message);
  }
  
  public void info(ParserRuleContext ctx, String message) {
    exec.info(ctx, message);
  }
}
