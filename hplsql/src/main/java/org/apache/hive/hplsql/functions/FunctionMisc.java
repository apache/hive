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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
    f.map.put("PART_COUNT_BY", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { partCountBy(ctx); }});
    
    f.specMap.put("ACTIVITY_COUNT", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { activityCount(ctx); }});
    f.specMap.put("CAST", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { cast(ctx); }});
    f.specMap.put("CURRENT", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { current(ctx); }});
    f.specMap.put("CURRENT_USER", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { currentUser(ctx); }});
    f.specMap.put("PART_COUNT", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { partCount(ctx); }});
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
  
  /**
   * PART_COUNT function
   */
  public void partCount(HplsqlParser.Expr_spec_funcContext ctx) {
    String tabname = evalPop(ctx.expr(0)).toString();
    StringBuilder sql = new StringBuilder();
    sql.append("SHOW PARTITIONS ");
    sql.append(tabname);    
    int cnt = ctx.expr().size();   
    if (cnt > 1) {
      sql.append(" PARTITION (");
      int i = 1;
      while (i + 1 < cnt) {
        String col = evalPop(ctx.expr(i)).toString();
        String val = evalPop(ctx.expr(i + 1)).toSqlString();
        if (i > 2) {
          sql.append(", ");
        }
        sql.append(col);
        sql.append("=");
        sql.append(val);        
        i += 2;
      }
      sql.append(")");
    }
    if (trace) {
      trace(ctx, "Query: " + sql);
    }
    if (exec.getOffline()) {
      evalNull();
      return;
    }
    Query query = exec.executeQuery(ctx, sql.toString(), exec.conf.defaultConnection);
    if (query.error()) {
      evalNullClose(query, exec.conf.defaultConnection);
      return;
    }
    int result = 0;
    ResultSet rs = query.getResultSet();
    try {
      while (rs.next()) {
        result++;
      }
    } catch (SQLException e) {
      evalNullClose(query, exec.conf.defaultConnection);
      return;
    }
    evalInt(result);
    exec.closeQuery(query, exec.conf.defaultConnection);
  }
  
  /**
   * PART_COUNT_BY function
   */
  public void partCountBy(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = ctx.func_param().size();
    if (cnt < 1 || exec.getOffline()) {
      return;
    }
    String tabname = evalPop(ctx.func_param(0).expr()).toString();
    ArrayList<String> keys = null;
    if (cnt > 1) {
      keys = new ArrayList<String>();
      for (int i = 1; i < cnt; i++) {
        keys.add(evalPop(ctx.func_param(i).expr()).toString().toUpperCase());
      }
    }    
    String sql = "SHOW PARTITIONS " + tabname;
    Query query = exec.executeQuery(ctx, sql, exec.conf.defaultConnection);
    if (query.error()) {
      exec.closeQuery(query, exec.conf.defaultConnection);
      return;
    }
    ResultSet rs = query.getResultSet();
    HashMap<String, Integer> group = new HashMap<String, Integer>();
    try {
      while (rs.next()) {
        String part = rs.getString(1);
        String[] parts = part.split("/");
        String key = parts[0];
        if (cnt > 1) {
          StringBuilder k = new StringBuilder();
          for (int i = 0; i < parts.length; i++) {
            if (keys.contains(parts[i].split("=")[0].toUpperCase())) {
              if (k.length() > 0) {
                k.append("/");
              }
              k.append(parts[i]);
            }
          }
          key = k.toString();
        }
        Integer count = group.get(key);
        if (count == null) {
          count = new Integer(0); 
        }
        group.put(key, count + 1);        
      }
    } catch (SQLException e) {
      exec.closeQuery(query, exec.conf.defaultConnection);
      return;
    }
    if (cnt == 1) {
      evalInt(group.size());
    }
    else {
      for (Map.Entry<String, Integer> i : group.entrySet()) {
        System.out.println(i.getKey() + '\t' + i.getValue());
      }
    }
    exec.closeQuery(query, exec.conf.defaultConnection);
  }
}
