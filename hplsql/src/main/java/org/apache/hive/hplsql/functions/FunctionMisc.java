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

package org.apache.hive.hplsql.functions;

import org.apache.hive.hplsql.*;

public class FunctionMisc extends Function {
  public FunctionMisc(Exec e) {
    super(e);
  }

  /** 
   * Register functions
   */
  @Override
  public void register(Function f) {
    f.map.put("COALESCE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { nvl(ctx); }});
    f.map.put("DECODE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { decode(ctx); }});
    f.map.put("NVL", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { nvl(ctx); }});
    f.map.put("NVL2", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { nvl2(ctx); }});
    
    f.specMap.put("ACTIVITY_COUNT", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { activityCount(ctx); }});
    f.specMap.put("CAST", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { cast(ctx); }});
    f.specMap.put("CURRENT", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { current(ctx); }});
    f.specMap.put("CURRENT_USER", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { currentUser(ctx); }});
    f.specMap.put("USER", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { currentUser(ctx); }});

    f.specSqlMap.put("CURRENT", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { currentSql(ctx); }});
  }
  
  /**
   * ACTIVITY_COUNT function (built-in variable)
   */
  void activityCount(HplsqlParser.Expr_spec_funcContext ctx) {
    evalInt(new Long(exec.getRowCount()));
  }
  
  /**
   * CAST function
   */
  void cast(HplsqlParser.Expr_spec_funcContext ctx) {
    if (ctx.expr().size() != 1) {
      evalNull();
      return;
    }
    String type = ctx.dtype().getText();
    String len = null;
    String scale = null;
    if (ctx.dtype_len() != null) {
      len = ctx.dtype_len().L_INT(0).getText();
      if (ctx.dtype_len().L_INT(1) != null) {
        scale = ctx.dtype_len().L_INT(1).getText();
      }
    }    
    Var var = new Var(null, type, len, scale, null);
    var.cast(evalPop(ctx.expr(0)));
    evalVar(var);
  }
  
  /**
   * CURRENT <VALUE> function
   */
  void current(HplsqlParser.Expr_spec_funcContext ctx) {
    if (ctx.T_DATE() != null) {
      evalVar(FunctionDatetime.currentDate()); 
    }
    else if (ctx.T_TIMESTAMP() != null) {
      int precision = evalPop(ctx.expr(0), 3).intValue();
      evalVar(FunctionDatetime.currentTimestamp(precision)); 
    }
    else if (ctx.T_USER() != null) {
      evalVar(FunctionMisc.currentUser());
    }
    else {
      evalNull();
    }
  }
  
  /**
   * CURRENT <VALUE> function in executable SQL statement
   */
  void currentSql(HplsqlParser.Expr_spec_funcContext ctx) {
    if (ctx.T_DATE() != null) {
      if (exec.getConnectionType() == Conn.Type.HIVE) {
        evalString("TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))");
      } 
      else {
        evalString("CURRENT_DATE");
      }
    }
    else if (ctx.T_TIMESTAMP() != null) {
      if (exec.getConnectionType() == Conn.Type.HIVE) {
        evalString("FROM_UNIXTIME(UNIX_TIMESTAMP())");
      } 
      else {
        evalString("CURRENT_TIMESTAMP");
      }
    }
    else {
      evalString(exec.getFormattedText(ctx));
    }
  }
  
  /**
   * CURRENT_USER function
   */
  void currentUser(HplsqlParser.Expr_spec_funcContext ctx) {
    evalVar(currentUser());
  }
  
  public static Var currentUser() {
    return new Var(System.getProperty("user.name"));
  }
  
  /**
   * DECODE function
   */
  void decode(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = ctx.func_param().size();
    if (cnt < 3) {
      evalNull();
      return;
    }
    Var value = evalPop(ctx.func_param(0).expr());
    int i = 1;
    while (i + 1 < cnt) {
      Var when = evalPop(ctx.func_param(i).expr());
      if ((value.isNull() && when.isNull()) || value.equals(when)) {
        eval(ctx.func_param(i + 1).expr());
        return;
      }
      i += 2;
    }    
    if (i < cnt) {           // ELSE expression
      eval(ctx.func_param(i).expr());
    }
    else {
      evalNull();
    }
  }
  
  /**
   * NVL function - Return first non-NULL expression
   */
  void nvl(HplsqlParser.Expr_func_paramsContext ctx) {
    for (int i=0; i < ctx.func_param().size(); i++) {
      Var v = evalPop(ctx.func_param(i).expr());
      if (v.type != Var.Type.NULL) {
        exec.stackPush(v);
        return;
      }
    }
    evalNull();
  }
  
  /**
   * NVL2 function - If expr1 is not NULL return expr2, otherwise expr3
   */
  void nvl2(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() == 3) {
      if (!evalPop(ctx.func_param(0).expr()).isNull()) {
        eval(ctx.func_param(1).expr());
      }
      else {
        eval(ctx.func_param(2).expr());
      }
    }
    else {
      evalNull();
    }
  }
}
