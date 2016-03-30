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

public class FunctionString extends Function {
  public FunctionString(Exec e) {
    super(e);
  }

  /** 
   * Register functions
   */
  @Override
  public void register(Function f) {
    f.map.put("CONCAT", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { concat(ctx); }});
    f.map.put("CHAR", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { char_(ctx); }});
    f.map.put("INSTR", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { instr(ctx); }});
    f.map.put("LEN", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { len(ctx); }});
    f.map.put("LENGTH", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { length(ctx); }});
    f.map.put("LOWER", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { lower(ctx); }});
    f.map.put("REPLACE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { replace(ctx); }}); 
    f.map.put("SUBSTR", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { substr(ctx); }});    
    f.map.put("SUBSTRING", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { substr(ctx); }});
    f.map.put("TO_CHAR", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { toChar(ctx); }});
    f.map.put("UPPER", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { upper(ctx); }});
    
    f.specMap.put("SUBSTRING", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { substring(ctx); }});
    f.specMap.put("TRIM", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { trim(ctx); }});
  }
  
  /**
   * CONCAT function
   */
  void concat(HplsqlParser.Expr_func_paramsContext ctx) {
    StringBuilder val = new StringBuilder();
    int cnt = getParamCount(ctx);
    boolean nulls = true;
    for (int i = 0; i < cnt; i++) {
      Var c = evalPop(ctx.func_param(i).expr());
      if (!c.isNull()) {
        val.append(c.toString());
        nulls = false;
      }
    }
    if (nulls) {
      evalNull();
    }
    else {
      evalString(val);
    }
  }
  
  /**
   * CHAR function
   */
  void char_(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = getParamCount(ctx);
    if (cnt != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString(); 
    evalString(str);
  }
  
  /**
   * INSTR function
   */
  void instr(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = getParamCount(ctx);
    if (cnt < 2) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString();
    if (str == null) {
      evalNull();
      return;
    }
    else if(str.isEmpty()) {
      evalInt(new Long(0));
      return;
    }
    String substr = evalPop(ctx.func_param(1).expr()).toString();
    int pos = 1;
    int occur = 1;
    int idx = 0;
    if (cnt >= 3) {
      pos = evalPop(ctx.func_param(2).expr()).intValue();
      if (pos == 0) {
        pos = 1;
      }
    }
    if (cnt >= 4) {
      occur = evalPop(ctx.func_param(3).expr()).intValue();
      if (occur < 0) {
        occur = 1;
      }
    }
    for (int i = occur; i > 0; i--) {
      if (pos > 0) {
        idx = str.indexOf(substr, pos - 1);
      }
      else {
        str = str.substring(0, str.length() - pos*(-1));
        idx = str.lastIndexOf(substr);
      }
      if (idx == -1) {
        idx = 0;
        break;
      }
      else {
        idx++;
      }
      if (i > 1) {
        if (pos > 0) {
          pos = idx + 1;
        }
        else {
          pos = (str.length() - idx + 1) * (-1);
        }
      }
    }
    evalInt(new Long(idx));
  }
  
  /**
   * LEN function (excluding trailing spaces)
   */
  void len(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    int len = evalPop(ctx.func_param(0).expr()).toString().trim().length(); 
    evalInt(new Long(len));
  }
  
  /**
   * LENGTH function
   */
  void length(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    int len = evalPop(ctx.func_param(0).expr()).toString().length(); 
    evalInt(new Long(len));
  }
  
  /**
   * LOWER function
   */
  void lower(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString().toLowerCase(); 
    evalString(str);
  }
  
  /**
   * REPLACE function
   */
  void replace(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = getParamCount(ctx);
    if (cnt < 3) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString(); 
    String what = evalPop(ctx.func_param(1).expr()).toString();
    String with = evalPop(ctx.func_param(2).expr()).toString();
    evalString(str.replaceAll(what, with));
  }
  
  /**
   * SUBSTR and SUBSTRING function
   */
  void substr(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = getParamCount(ctx);
    if (cnt < 2) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString(); 
    int start = evalPop(ctx.func_param(1).expr()).intValue();
    int len = -1;
    if (start == 0) {
      start = 1; 
    }
    if (cnt > 2) {
      len = evalPop(ctx.func_param(2).expr()).intValue();
    }
    substr(str, start, len);
  }
  
  void substr(String str, int start, int len) {
    if (str == null) {
      evalNull();
      return;
    }
    else if (str.isEmpty()) {
      evalString(str);
      return;
    }
    if (start == 0) {
      start = 1; 
    }
    if (len == -1) {
      if (start > 0) {
        evalString(str.substring(start - 1));
      }
    }
    else {
      evalString(str.substring(start - 1, start - 1 + len));      
    }
  }
  
  /**
   * SUBSTRING FROM FOR function
   */
  void substring(HplsqlParser.Expr_spec_funcContext ctx) {
    String str = evalPop(ctx.expr(0)).toString(); 
    int start = evalPop(ctx.expr(1)).intValue();
    int len = -1;
    if (start == 0) {
      start = 1; 
    }
    if (ctx.T_FOR() != null) {
      len = evalPop(ctx.expr(2)).intValue();
    }
    substr(str, start, len);
  }
  
  /**
   * TRIM function
   */
  void trim(HplsqlParser.Expr_spec_funcContext ctx) {
    int cnt = ctx.expr().size();
    if (cnt != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.expr(0)).toString(); 
    evalString(str.trim());
  }
  
  /**
   * TO_CHAR function
   */
  void toChar(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = getParamCount(ctx);
    if (cnt != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString(); 
    evalString(str);
  }
  
  /**
   * UPPER function
   */
  void upper(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString().toUpperCase(); 
    evalString(str);
  }
}
