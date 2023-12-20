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
    f.map.put("LEN", this::len);
    f.map.put("TO_CHAR", this::toChar);
    f.map.put("UPPER", this::upper);
  }
  
  /**
   * CONCAT function
   */
  void concat(HplsqlParser.Expr_func_paramsContext ctx) {
    StringBuilder val = new StringBuilder("'");
    int cnt = getParamCount(ctx);
    boolean nulls = true;
    for (int i = 0; i < cnt; i++) {
      Var c = evalPop(ctx.func_param(i).expr());
      if (!c.isNull() && !"null".equalsIgnoreCase((String)c.value)) {
        val.append(Utils.unquoteString(c.toString()));
        nulls = false;
      }
    }
    if (nulls) {
      evalNull();
    }
    else {
      val.append("'");
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
   * LEN function (excluding trailing spaces)
   */
  void len(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    int len = Utils.unquoteString(evalPop(ctx.func_param(0).expr()).toString()).trim().length();
    evalInt(len);
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
