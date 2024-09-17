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

import org.apache.hive.hplsql.*;
import org.apache.hive.hplsql.executor.QueryExecutor;

public class FunctionString extends BuiltinFunctions {
  public FunctionString(Exec e, QueryExecutor queryExecutor) {
    super(e, queryExecutor);
  }

  /** 
   * Register functions
   */
  @Override
  public void register(BuiltinFunctions f) {
    f.map.put("CONCAT", this::concat);
    f.map.put("CHAR", this::char_);
    f.map.put("INSTR", this::instr);
    f.map.put("LEN", this::len);
    f.map.put("LENGTH", this::length);
    f.map.put("LOWER", this::lower);
    f.map.put("REPLACE", this::replace);
    f.map.put("SUBSTR", this::substr);
    f.map.put("SUBSTRING", this::substr);
    f.map.put("TO_CHAR", this::toChar);
    f.map.put("UPPER", this::upper);

    f.specMap.put("SUBSTRING", this::substring);
    f.specMap.put("TRIM", this::trim);
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
        String value = c.toString();
        value = unquoteString(value);
        val.append(value);
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

  private String unquoteString(String value) {
    if (exec.buildSql) {
      value = Utils.unquoteString(value);
    }
    return value;
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
    str = unquoteString(str);
    if (str == null) {
      evalNull();
      return;
    }
    else if(str.isEmpty()) {
      evalInt(0);
      return;
    }
    String substr = evalPop(ctx.func_param(1).expr()).toString();
    substr = unquoteString(substr);
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
    evalInt(idx);
  }
  
  /**
   * LEN function (excluding trailing spaces)
   */
  void len(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    String value = evalPop(ctx.func_param(0).expr()).toString();
    value = unquoteString(value);
    int len = value.trim().length();
    evalInt(len);
  }

  /**
   * LENGTH function
   */
  void length(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    String value = evalPop(ctx.func_param(0).expr()).toString();
    value = unquoteString(value);
    int len = value.length();
    evalInt(len);
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
    str = unquoteString(str);
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
    what = unquoteString(what);
    String with = evalPop(ctx.func_param(2).expr()).toString();
    with = unquoteString(with);
    evalString(str.replaceAll(what, with));
  }

  /**
   * SUBSTR and SUBSTRING function
   */
  void substr(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = BuiltinFunctions.getParamCount(ctx);
    if (cnt < 2) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString();
    str = unquoteString(str);
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
    str = unquoteString(str);
    evalString(str.trim());
  }
  
  /**
   * TO_CHAR function
   */
  void toChar(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = BuiltinFunctions.getParamCount(ctx);
    if (cnt != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString();
    evalSqlString(str);
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
    str = unquoteString(str);
    evalString(str);
  }
}
