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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hplsql;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hplsql.executor.QueryExecutor;
import org.apache.hive.hplsql.executor.QueryResult;

public class Copy {
  
  Exec exec;
  Timer timer = new Timer();
  boolean trace = false;
  boolean info = false;
  
  long srcSizeInBytes = 0;
  
  String delimiter = "\t";
  boolean sqlInsert = false;
  String sqlInsertName;
  String targetConn;
  int batchSize = 1000;
  
  boolean overwrite = false;
  boolean delete = false;
  boolean ignore = false;
  private QueryExecutor queryExecutor;

  Copy(Exec e, QueryExecutor queryExecutor) {
    exec = e;
    trace = exec.getTrace();
    info = exec.getInfo();
    this.queryExecutor = queryExecutor;
  }
  
  /**
   * Run COPY command
   */
  Integer run(HplsqlParser.Copy_stmtContext ctx) {
    trace(ctx, "COPY");
    initOptions(ctx);
    StringBuilder sql = new StringBuilder();
    if (ctx.table_name() != null) {
      String table = evalPop(ctx.table_name()).toString();
      sql.append("SELECT * FROM ");
      sql.append(table); 
    }
    else {
      sql.append(evalPop(ctx.select_stmt()).toString());
      if (trace) {
        trace(ctx, "Statement:\n" + sql);
      }
    }
    QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
    if (query.error()) { 
      exec.signal(query);
      return 1;
    }    
    exec.setSqlSuccess();
    try {
      if (targetConn != null) {
        copyToTable(ctx, query);
      }
      else {
        copyToFile(ctx, query);
      }
    } catch (Exception e) {
      exec.signal(e);
      return 1;
    } finally {
      query.close();
    }
    return 0;
  }
    
  /**
   * Copy the query results to another table
   * @throws Exception 
   */
  void copyToTable(HplsqlParser.Copy_stmtContext ctx, QueryResult query) throws Exception {
    int cols = query.columnCount();
    int rows = 0;
    if (trace) {
      trace(ctx, "SELECT executed: " + cols + " columns");
    } 
    Connection conn = exec.getConnection(targetConn);
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO " + sqlInsertName + " VALUES (");
    for (int i = 0; i < cols; i++) {
      sql.append("?");
      if (i + 1 < cols) {
        sql.append(",");
      }
    }
    sql.append(")");
    PreparedStatement ps = conn.prepareStatement(sql.toString());
    long start = timer.start();
    long prev = start;
    boolean batchOpen = false;
    while (query.next()) {
      for (int i = 0; i < cols; i++) {
        ps.setObject(i, query.column(i, Object.class));
      }
      rows++;
      if (batchSize > 1) {
        ps.addBatch();
        batchOpen = true;
        if (rows % batchSize == 0) {
          ps.executeBatch();
          batchOpen = false;
        }        
      }
      else {
        ps.executeUpdate();
      }      
      if (trace && rows % 100 == 0) {
        long cur = timer.current();
        if (cur - prev > 10000) {
          trace(ctx, "Copying rows: " + rows + " (" + rows/((cur - start)/1000) + " rows/sec)");
          prev = cur;
        }
      }
    }
    if (batchOpen) {
      ps.executeBatch();
    }
    ps.close();
    exec.returnConnection(targetConn, conn);
    exec.setRowCount(rows);
    long elapsed = timer.stop();
    if (info) {
      DecimalFormat df = new DecimalFormat("#,##0.00");
      df.setRoundingMode(RoundingMode.HALF_UP);
      info(ctx, "COPY completed: " + rows + " row(s), " + timer.format() + ", " + df.format(rows/(elapsed/1000.0)) + " rows/sec");
    }
  }
  
  /**
   * Copy the query results to a file
   * @throws Exception 
   */
  void copyToFile(HplsqlParser.Copy_stmtContext ctx, QueryResult query) throws Exception {
    String filename = null;
    if (ctx.copy_target().expr() != null) {
      filename = evalPop(ctx.copy_target().expr()).toString();
    }
    else {
      filename = ctx.copy_target().getText();
    }
    byte[] del = delimiter.getBytes();
    byte[] rowdel = "\n".getBytes();
    byte[] nullstr = "NULL".getBytes();
    int cols = query.columnCount();
    int rows = 0;
    long bytes = 0;
    if (trace || info) {
      String mes = "Query executed: " + cols + " columns, output file: " + filename;
      if (trace) {
        trace(ctx, mes);
      }
      else {
        info(ctx, mes);
      }
    } 
    java.io.File file = null;
    File hdfsFile = null;
    if (ctx.T_HDFS() == null) {
      file = new java.io.File(filename);
    }
    else {
      hdfsFile = new File();
    }     
    OutputStream out = null;
    timer.start();
    try {      
      if (file != null) {
        if (!file.exists()) {
          file.createNewFile();
        }
        out = new FileOutputStream(file, false /*append*/);
      }
      else {
        out = hdfsFile.create(filename, true /*overwrite*/);
      }
      String col;
      String sql = "";
      if (sqlInsert) {
        sql = "INSERT INTO " + sqlInsertName + " VALUES (";
        rowdel = ");\n".getBytes();
      }
      while (query.next()) {
        if (sqlInsert) {
          out.write(sql.getBytes());
        }
        for (int i = 0; i < cols; i++) {
          if (i > 0) {
            out.write(del);
            bytes += del.length;
          }
          col = query.column(i, String.class);
          if (col != null) {
            if (sqlInsert) {
              col = Utils.quoteString(col);
            }
            byte[] b = col.getBytes();
            out.write(b);
            bytes += b.length;
          }
          else if (sqlInsert) {
            out.write(nullstr);
          }
        }
        out.write(rowdel);
        bytes += rowdel.length;
        rows++;
      }
      exec.setRowCount(rows);
    }
    finally {
      if (out != null) {
        out.close();
      }
    }
    long elapsed = timer.stop();
    if (info) {
      DecimalFormat df = new DecimalFormat("#,##0.00");
      df.setRoundingMode(RoundingMode.HALF_UP);
      info(ctx, "COPY completed: " + rows + " row(s), " + Utils.formatSizeInBytes(bytes) + ", " + timer.format() + ", " + df.format(rows/(elapsed/1000.0)) + " rows/sec");
    }
  }
  
  /**
   * Initialize COPY command options
   */
  void initOptions(HplsqlParser.Copy_stmtContext ctx) {
    int cnt = ctx.copy_option().size();
    for (int i = 0; i < cnt; i++) {
      HplsqlParser.Copy_optionContext option = ctx.copy_option(i);
      if (option.T_DELIMITER() != null) {
        delimiter = StringEscapeUtils.unescapeJava(evalPop(option.expr()).toString());
      }
      else if (option.T_SQLINSERT() != null) {
        sqlInsert = true;
        delimiter = ", ";
        if (option.qident() != null) {
          sqlInsertName = option.qident().getText();
        }
      }
      else if (option.T_AT() != null) {
        targetConn = option.qident().getText();
        if (ctx.copy_target().expr() != null) {
          sqlInsertName = evalPop(ctx.copy_target().expr()).toString();
        }
        else {
          sqlInsertName = ctx.copy_target().getText();
        }
      }
      else if (option.T_BATCHSIZE() != null) {
        batchSize = evalPop(option.expr()).intValue();
      }
    }
  }
 
  /**
   * Evaluate the expression and pop value from the stack
   */
  Var evalPop(ParserRuleContext ctx) {
    exec.visit(ctx);
    if (!exec.stack.isEmpty()) { 
      return exec.stackPop();
    }
    return Var.Empty;
  }

  /**
   * Trace and information
   */
  public void trace(ParserRuleContext ctx, String message) {
    exec.trace(ctx, message);
  }
  
  public void info(ParserRuleContext ctx, String message) {
    exec.info(ctx, message);
  }
}
