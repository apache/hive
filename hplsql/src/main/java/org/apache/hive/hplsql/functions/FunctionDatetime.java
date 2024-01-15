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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.hplsql.*;
import org.apache.hive.hplsql.executor.QueryExecutor;

public class FunctionDatetime extends BuiltinFunctions {
  public FunctionDatetime(Exec e, QueryExecutor queryExecutor) {
    super(e, queryExecutor);
  }

  /** 
   * Register functions
   */
  @Override
  public void register(BuiltinFunctions f) {
    f.map.put("DATE", this::date);
    f.map.put("NOW", ctx -> now(ctx));
    f.map.put("TIMESTAMP_ISO", this::timestampIso);
    f.map.put("TO_TIMESTAMP", this::toTimestamp);
    f.map.put("CURRENT_TIME_MILLIS", this::currentTimeMillis);

    f.specMap.put("SYSDATE", this::currentTimestamp);

    f.specSqlMap.put("CURRENT_DATE", (FuncSpecCommand) this::currentDateSql);
    f.specSqlMap.put("CURRENT_TIMESTAMP", (FuncSpecCommand) this::currentTimestampSql);
 }
  
  /**
   * CURRENT_DATE
   */
  public static Var currentDate() {
    SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
    String s = f.format(Calendar.getInstance().getTime());
    return new Var(Var.Type.DATE, Utils.toDate(s)); 
  }
  
  /**
   * CURRENT_DATE in executable SQL statement
   */
  public void currentDateSql(HplsqlParser.Expr_spec_funcContext ctx) {
    if (exec.getConnectionType() == Conn.Type.HIVE) {
      evalString("TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))");
    } 
    else {
      evalString(exec.getFormattedText(ctx));
    }
  }
  
  /**
   * CURRENT_TIMESTAMP
   */
  public void currentTimestamp(HplsqlParser.Expr_spec_funcContext ctx) {
    int precision = evalPop(ctx.expr(0), 3).intValue();
    evalVar(currentTimestamp(precision));
  }

  public static Var currentTimestamp(int precision) {
    String format = "yyyy-MM-dd HH:mm:ss";
    if (precision > 0 && precision <= 3) {
      format += "." + StringUtils.repeat("S", precision);
    }
    SimpleDateFormat f = new SimpleDateFormat(format);
    String s = f.format(Calendar.getInstance(TimeZone.getDefault()).getTime());
    return new Var(Utils.toTimestamp(s), precision); 
  }
  
  /**
   * CURRENT_TIMESTAMP in executable SQL statement
   */
  public void currentTimestampSql(HplsqlParser.Expr_spec_funcContext ctx) {
    if (exec.getConnectionType() == Conn.Type.HIVE) {
      evalString("FROM_UNIXTIME(UNIX_TIMESTAMP())");
    } 
    else {
      evalString(Exec.getFormattedText(ctx));
    }
  }
  
  /**
   * DATE function
   */
  void date(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    Var var = new Var(Var.Type.DATE);
    Var date = evalPop(ctx.func_param(0).expr());
    date.setValue(Utils.unquoteString(date.toString()));
    var.cast(date);
    evalVar(var);
  }
  
  /**
   * NOW() function (current date and time)
   */
  void now(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx != null) {
      evalNull();
      return;
    }
    evalVar(currentTimestamp(3));
  }

  /**
   * TIMESTAMP_ISO function
   */
  void timestampIso(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    Var var = new Var(Var.Type.TIMESTAMP);
    Var val = evalPop(ctx.func_param(0).expr());
    val.setValue(Utils.unquoteString(val.toString()));
    var.cast(val);
    evalVar(var);
  }
  
  /**
   * TO_TIMESTAMP function
   */
  void toTimestamp(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 2) {
      evalNull();
      return;
    }    
    String value = Utils.unquoteString(evalPop(ctx.func_param(0).expr()).toString());
    String sqlFormat = Utils.unquoteString(evalPop(ctx.func_param(1).expr()).toString());
    String format = Utils.convertSqlDatetimeFormat(sqlFormat);
    try {
      long timeInMs = new SimpleDateFormat(format).parse(value).getTime();
      evalVar(new Var(Var.Type.TIMESTAMP, new Timestamp(timeInMs)));
    }
    catch (Exception e) {
      exec.signal(e);
      evalNull();
    }
  }

  public void currentTimeMillis(HplsqlParser.Expr_func_paramsContext ctx) {
    evalVar(new Var(System.currentTimeMillis()));
  }
}  
