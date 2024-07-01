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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
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
    f.map.put("FROM_UNIXTIME", this::fromUnixtime);
    f.map.put("NOW", ctx -> now(ctx));
    f.map.put("TIMESTAMP_ISO", this::timestampIso);
    f.map.put("TO_TIMESTAMP", this::toTimestamp);
    f.map.put("UNIX_TIMESTAMP", this::unixTimestamp);
    f.map.put("CURRENT_TIME_MILLIS", this::currentTimeMillis);

    f.specMap.put("CURRENT_DATE", this::currentDate);
    f.specMap.put("CURRENT_TIMESTAMP", this::currentTimestamp);
    f.specMap.put("SYSDATE", this::currentTimestamp);

    f.specSqlMap.put("CURRENT_DATE", (FuncSpecCommand) this::currentDateSql);
    f.specSqlMap.put("CURRENT_TIMESTAMP", (FuncSpecCommand) this::currentTimestampSql);
 }

  private static DateTimeFormatter createDateTimeFormatter(String format) {
    return DateTimeFormatter.ofPattern(format).withZone(TimeZone.getTimeZone("UTC").toZoneId())
        .withResolverStyle(ResolverStyle.STRICT);
  }

  /**
   * CURRENT_DATE
   */
  public void currentDate(HplsqlParser.Expr_spec_funcContext ctx) {
    evalVar(currentDate());
  }
  
  /**
   * CURRENT_DATE
   */
  public static Var currentDate() {
    DateTimeFormatter formatter = createDateTimeFormatter("yyyy-MM-dd");
    String date = formatter.format(new java.sql.Date(System.currentTimeMillis()).toLocalDate());
    return new Var(Var.Type.DATE, Utils.toDate(date));
  }
  
  /**
   * CURRENT_DATE in executable SQL statement
   */
  public void currentDateSql(HplsqlParser.Expr_spec_funcContext ctx) {
    if (exec.getConnectionType() == Conn.Type.HIVE) {
      evalSqlString("TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))");
    } 
    else {
      evalSqlString(exec.getFormattedText(ctx));
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
    DateTimeFormatter formatter = createDateTimeFormatter(format);
    String timestamp = formatter.format(new java.sql.Timestamp(System.currentTimeMillis()).toLocalDateTime());
    return new Var(Utils.toTimestamp(timestamp), precision);
  }
  
  /**
   * CURRENT_TIMESTAMP in executable SQL statement
   */
  public void currentTimestampSql(HplsqlParser.Expr_spec_funcContext ctx) {
    if (exec.getConnectionType() == Conn.Type.HIVE) {
      evalSqlString("FROM_UNIXTIME(UNIX_TIMESTAMP())");
    } 
    else {
      evalSqlString(Exec.getFormattedText(ctx));
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
      DateTimeFormatter formatter = createDateTimeFormatter(format);
      LocalDateTime val = null;
      if (format.length() > 10) {
        val = LocalDateTime.parse(value, formatter);
      } else {
        val = LocalDate.parse(value, formatter).atStartOfDay();
      }
      evalVar(new Var(Var.Type.TIMESTAMP, Timestamp.valueOf(val)));
    }
    catch (Exception e) {
      exec.signal(e);
      evalNull();
    }
  }

  /**
   * FROM_UNIXTIME() function (convert seconds since 1970-01-01 00:00:00 to timestamp)
   */
  void fromUnixtime(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = BuiltinFunctions.getParamCount(ctx);
    if (cnt == 0) {
      evalNull();
      return;
    }
    Var value = evalPop(ctx.func_param(0).expr());
    if (value.type != Var.Type.BIGINT) {
      Var newVar = new Var(Var.Type.BIGINT);
      value = newVar.cast(value);
    }
    long epoch = value.longValue();
    String format = "yyyy-MM-dd HH:mm:ss";
    if (cnt > 1) {
      format = Utils.unquoteString(evalPop(ctx.func_param(1).expr()).toString());
    }
    DateTimeFormatter formatter = createDateTimeFormatter(format);
    evalString(formatter.format(new java.sql.Date(epoch * 1000).toLocalDate()));
  }

  /**
   * UNIX_TIMESTAMP() function (current date and time in seconds since 1970-01-01 00:00:00)
   */
  void unixTimestamp(HplsqlParser.Expr_func_paramsContext ctx) {
    evalVar(new Var(System.currentTimeMillis()/1000));
  }

  public void currentTimeMillis(HplsqlParser.Expr_func_paramsContext ctx) {
    evalVar(new Var(System.currentTimeMillis()));
  }
}  
