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

package org.apache.hive.hplsql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Stack;
import java.util.UUID;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.hive.hplsql.Var.Type;
import org.apache.hive.hplsql.HplsqlParser.Create_table_columns_itemContext;
import org.apache.hive.hplsql.HplsqlParser.Create_table_columnsContext;

/**
 * HPL/SQL statements execution
 */
public class Stmt {

  Exec exec = null;
  Stack<Var> stack = null;
  Conf conf;
  Meta meta;
  
  boolean trace = false; 
  
  Stmt(Exec e) {
    exec = e;  
    stack = exec.getStack();
    conf = exec.getConf();
    meta = exec.getMeta();
    trace = exec.getTrace();
  }
  
  /**
   * ALLOCATE CURSOR statement
   */
  public Integer allocateCursor(HplsqlParser.Allocate_cursor_stmtContext ctx) { 
    trace(ctx, "ALLOCATE CURSOR");
    String name = ctx.ident(0).getText();
    Var cur = null;
    if (ctx.T_PROCEDURE() != null) {
      cur = exec.consumeReturnCursor(ctx.ident(1).getText());
    }
    else if (ctx.T_RESULT() != null) {
      cur = exec.findVariable(ctx.ident(1).getText());
      if (cur != null && cur.type != Type.RS_LOCATOR) {
        cur = null;
      }
    }
    if (cur == null) {
      trace(ctx, "Cursor for procedure not found: " + name);
      exec.signal(Signal.Type.SQLEXCEPTION);
      return -1;
    }
    exec.addVariable(new Var(name, Type.CURSOR, cur.value)); 
    return 0; 
  }
  
  /**
   * ASSOCIATE LOCATOR statement
   */
  public Integer associateLocator(HplsqlParser.Associate_locator_stmtContext ctx) { 
    trace(ctx, "ASSOCIATE LOCATOR");
    int cnt = ctx.ident().size();
    if (cnt < 2) {
      return -1;
    }
    String procedure = ctx.ident(cnt - 1).getText();
    for (int i = 0; i < cnt - 1; i++) {
      Var cur = exec.consumeReturnCursor(procedure);
      if (cur != null) {
        String name = ctx.ident(i).getText(); 
        Var loc = exec.findVariable(name);
        if (loc == null) {
          loc = new Var(name, Type.RS_LOCATOR, cur.value);
          exec.addVariable(loc);
        }
        else {
          loc.setValue(cur.value);
        }
      }      
    }
    return 0; 
  }
  
  /**
   * DECLARE cursor statement
   */
  public Integer declareCursor(HplsqlParser.Declare_cursor_itemContext ctx) { 
    String name = ctx.ident().getText();
    if (trace) {
      trace(ctx, "DECLARE CURSOR " + name);
    }
    Query query = new Query();
    if (ctx.expr() != null) {
      query.setExprCtx(ctx.expr());
    }
    else if (ctx.select_stmt() != null) {
      query.setSelectCtx(ctx.select_stmt());
    }
    if (ctx.cursor_with_return() != null) {
      query.setWithReturn(true);
    }
    Var var = new Var(name, Type.CURSOR, query);
    exec.addVariable(var);
    return 0; 
  }
  
  /**
   * CREATE TABLE statement
   */
  public Integer createTable(HplsqlParser.Create_table_stmtContext ctx) { 
    trace(ctx, "CREATE TABLE");
    StringBuilder sql = new StringBuilder();
    sql.append(exec.getText(ctx, ctx.T_CREATE().getSymbol(), ctx.T_TABLE().getSymbol()));
    sql.append(" " + evalPop(ctx.table_name()) + " (");
    int cnt = ctx.create_table_columns().create_table_columns_item().size();
    int cols = 0;
    for (int i = 0; i < cnt; i++) {
      Create_table_columns_itemContext col = ctx.create_table_columns().create_table_columns_item(i);
      if (col.create_table_column_cons() != null) {
        continue;
      }
      if (cols > 0) {
        sql.append(",\n");
      }
      sql.append(evalPop(col.column_name()));
      sql.append(" ");
      sql.append(exec.evalPop(col.dtype(), col.dtype_len()));
      cols++;
    }
    sql.append("\n)");
    if (ctx.create_table_options() != null) {
      String opt = evalPop(ctx.create_table_options()).toString();
      if (opt != null) {
        sql.append(" " + opt);
      }
    }
    trace(ctx, sql.toString());
    Query query = exec.executeSql(ctx, sql.toString(), exec.conf.defaultConnection);
    if (query.error()) {
      exec.signal(query);
      return 1;
    }
    exec.setSqlSuccess();
    exec.closeQuery(query, exec.conf.defaultConnection);
    return 0; 
  }  

  /**
   * CREATE TABLE options for Hive
   */
  public Integer createTableHiveOptions(HplsqlParser.Create_table_options_hive_itemContext ctx) {
    if (ctx.create_table_hive_row_format() != null) {
      createTableHiveRowFormat(ctx.create_table_hive_row_format());
    }
    return 0; 
  }
  
  public Integer createTableHiveRowFormat(HplsqlParser.Create_table_hive_row_formatContext ctx) {
    StringBuilder sql = new StringBuilder();
    sql.append("ROW FORMAT DELIMITED");
    int cnt = ctx.create_table_hive_row_format_fields().size();
    for (int i = 0; i < cnt; i++) {
      HplsqlParser.Create_table_hive_row_format_fieldsContext c = ctx.create_table_hive_row_format_fields(i);
      if (c.T_FIELDS() != null) {
        sql.append(" FIELDS TERMINATED BY " + evalPop(c.expr(0)).toSqlString());
      }
      else if (c.T_LINES() != null) {
        sql.append(" LINES TERMINATED BY " + evalPop(c.expr(0)).toSqlString());
      } 
    }
    evalString(sql);
    return 0; 
  }
    
  /**
   * DECLARE TEMPORARY TABLE statement 
   */
  public Integer declareTemporaryTable(HplsqlParser.Declare_temporary_table_itemContext ctx) { 
    String name = ctx.ident().getText();
    if (trace) {
      trace(ctx, "DECLARE TEMPORARY TABLE " + name);
    }
    return createTemporaryTable(ctx, ctx.create_table_columns(), name);
  }
  
  /**
   * CREATE LOCAL TEMPORARY | VOLATILE TABLE statement 
   */
  public Integer createLocalTemporaryTable(HplsqlParser.Create_local_temp_table_stmtContext ctx) { 
    String name = ctx.ident().getText();
    if (trace) {
      trace(ctx, "CREATE LOCAL TEMPORARY TABLE " + name);
    }
    return createTemporaryTable(ctx, ctx.create_table_columns(), name);
   }
  
  /**
   * Create a temporary table statement 
   */
  public Integer createTemporaryTable(ParserRuleContext ctx, Create_table_columnsContext colCtx, String name) { 
    String managedName = null;
    String sql = null;
    String columns = exec.getFormattedText(colCtx);
    if (conf.tempTables == Conf.TempTables.NATIVE) {
      sql = "CREATE TEMPORARY TABLE " + name + "\n(" + columns + "\n)";
    } else if (conf.tempTables == Conf.TempTables.MANAGED) {
      managedName = name + "_" + UUID.randomUUID().toString().replace("-","");
      if (!conf.tempTablesSchema.isEmpty()) {
        managedName = conf.tempTablesSchema + "." + managedName;
      }      
      sql = "CREATE TABLE " + managedName + "\n(" + columns + "\n)";
      if (!conf.tempTablesLocation.isEmpty()) {
        sql += "\nLOCATION '" + conf.tempTablesLocation + "/" + managedName + "'";
      }
      if (trace) {
        trace(ctx, "Managed table name: " + managedName);
      }
    }  
    if (sql != null) {
      Query query = exec.executeSql(ctx, sql, exec.conf.defaultConnection);
      if (query.error()) {
        exec.signal(query);
        return 1;
      }
      if (managedName != null) {
        exec.addManagedTable(name, managedName);
      }
      exec.setSqlSuccess();
      exec.closeQuery(query, exec.conf.defaultConnection);
    }    
    return 0; 
  }
  
  /**
   * DROP statement
   */
  public Integer drop(HplsqlParser.Drop_stmtContext ctx) { 
    trace(ctx, "DROP");
    String sql = null;    
    if (ctx.T_TABLE() != null) {
      sql = "DROP TABLE ";
      if (ctx.T_EXISTS() != null) {
        sql += "IF NOT EXISTS ";
      }
      sql += evalPop(ctx.table_name()).toString();
    }
    if (sql != null) {
      trace(ctx, sql);
      Query query = exec.executeSql(ctx, sql, exec.conf.defaultConnection);
      if (query.error()) {
        exec.signal(query);
        return 1;
      }
      exec.setSqlSuccess();
      exec.closeQuery(query, exec.conf.defaultConnection);
    }
    return 0; 
  }
  
  /**
   * OPEN cursor statement
   */
  public Integer open(HplsqlParser.Open_stmtContext ctx) { 
    trace(ctx, "OPEN");
    Query query = null;
    Var var = null;
    String cursor = ctx.L_ID().toString();   
    String sql = null;
    if (ctx.T_FOR() != null) {                             // SELECT statement or dynamic SQL
      if (ctx.expr() != null) {
        sql = evalPop(ctx.expr()).toString();
      }
      else {
        sql = evalPop(ctx.select_stmt()).toString();
      }
      query = new Query(sql);
      var = exec.findCursor(cursor);                      // Can be a ref cursor variable
      if (var == null) {
        var = new Var(cursor, Type.CURSOR, query);
        exec.addVariable(var);
      }
      else {
        var.setValue(query);
      }
    }
    else {                                                 // Declared cursor
      var = exec.findVariable(cursor);      
      if (var != null && var.type == Type.CURSOR) {
        query = (Query)var.value;
        if (query.sqlExpr != null) {
          sql = evalPop(query.sqlExpr).toString();
          query.setSql(sql);
        }
        else if (query.sqlSelect != null) {
          sql = evalPop(query.sqlSelect).toString();
          query.setSql(sql);
        }
      }
    }
    if (query != null) {
      if (trace) {
        trace(ctx, cursor + ": " + sql);
      } 
      exec.executeQuery(ctx, query, exec.conf.defaultConnection);
      if (query.error()) {
        exec.signal(query);
        return 1;
      }
      else if (!exec.getOffline()) {
        exec.setSqlCode(0);
      }
      if (query.getWithReturn()) {
        exec.addReturnCursor(var);
      }
    }
    else {
      trace(ctx, "Cursor not found: " + cursor);
      exec.setSqlCode(-1);
      exec.signal(Signal.Type.SQLEXCEPTION);
      return 1;
    }
    return 0; 
  }
  
  /**
   * FETCH cursor statement
   */
  public Integer fetch(HplsqlParser.Fetch_stmtContext ctx) { 
    trace(ctx, "FETCH");
    String name = ctx.L_ID(0).toString();
    Var cursor = exec.findCursor(name);
    if (cursor == null) {
      trace(ctx, "Cursor not found: " + name);
      exec.setSqlCode(-1);
      exec.signal(Signal.Type.SQLEXCEPTION);
      return 1;
    }  
    else if (cursor.value == null) {
      trace(ctx, "Cursor not open: " + name);
      exec.setSqlCode(-1);
      exec.signal(Signal.Type.SQLEXCEPTION);
      return 1;
    }  
    else if (exec.getOffline()) {
      exec.setSqlCode(100);
      exec.signal(Signal.Type.NOTFOUND);
      return 0;
    }
    // Assign values from the row to local variables
    try {
      Query query = (Query)cursor.value;
      ResultSet rs = query.getResultSet();
      ResultSetMetaData rsm = null;
      if(rs != null) {
        rsm = rs.getMetaData();
      }
      if(rs != null && rsm != null) {
        int cols = ctx.L_ID().size() - 1;
        if(rs.next()) {
          query.setFetch(true);
          for(int i=1; i <= cols; i++) {
            Var var = exec.findVariable(ctx.L_ID(i).getText());
            if(var != null) {
              if (var.type != Var.Type.ROW) {
                var.setValue(rs, rsm, i);
              }
              else {
                var.setValues(rs, rsm);
              }
              if (trace) {
                trace(ctx, var, rs, rsm, i);
              }
            } 
            else if(trace) {
              trace(ctx, "Variable not found: " + ctx.L_ID(i).getText());
            }
          }
          exec.incRowCount();
          exec.setSqlSuccess();
        }
        else {
          query.setFetch(false);
          exec.setSqlCode(100);
        }
      }
      else {
        exec.setSqlCode(-1);
      }
    } 
    catch (SQLException e) {
      exec.setSqlCode(e);
      exec.signal(Signal.Type.SQLEXCEPTION, e.getMessage(), e);
    } 
    return 0; 
  }  
  
  /**
   * CLOSE cursor statement
   */
  public Integer close(HplsqlParser.Close_stmtContext ctx) { 
    trace(ctx, "CLOSE");
    String name = ctx.L_ID().toString();
    Var var = exec.findVariable(name);
    if(var != null && var.type == Type.CURSOR) {
      exec.closeQuery((Query)var.value, exec.conf.defaultConnection);
      exec.setSqlCode(0);
    }
    else if(trace) {
      trace(ctx, "Cursor not found: " + name);
    }
    return 0; 
  }
  
  /**
   * INCLUDE statement
   */
  public Integer include(HplsqlParser.Include_stmtContext ctx) {
    String file;
    if (ctx.file_name() != null) {
      file = ctx.file_name().getText();
    }
    else {
      file = evalPop(ctx.expr()).toString();
    }    
    trace(ctx, "INCLUDE " + file);
    exec.includeFile(file);
    return 0; 
  }
  
  /**
   * IF statement (PL/SQL syntax)
   */
  public Integer ifPlsql(HplsqlParser.If_plsql_stmtContext ctx) {
    boolean trueExecuted = false;
    trace(ctx, "IF");
    if (evalPop(ctx.bool_expr()).isTrue()) {
      trace(ctx, "IF TRUE executed");
      visit(ctx.block());
      trueExecuted = true;
    }
    else if (ctx.elseif_block() != null) {
      int cnt = ctx.elseif_block().size();
      for (int i = 0; i < cnt; i++) {
        if (evalPop(ctx.elseif_block(i).bool_expr()).isTrue()) {
          trace(ctx, "ELSE IF executed");
          visit(ctx.elseif_block(i).block());
          trueExecuted = true;
          break;
        }
      }
    }
    if (!trueExecuted && ctx.else_block() != null) {
      trace(ctx, "ELSE executed");
      visit(ctx.else_block());
    }
    return 0; 
  }
  
  /**
   * IF statement (Transact-SQL syntax)
   */
  public Integer ifTsql(HplsqlParser.If_tsql_stmtContext ctx) {
    trace(ctx, "IF");
    visit(ctx.bool_expr());
    if(exec.stackPop().isTrue()) {
      trace(ctx, "IF TRUE executed");
      visit(ctx.single_block_stmt(0));
    }
    else if(ctx.T_ELSE() != null) {
      trace(ctx, "ELSE executed");
      visit(ctx.single_block_stmt(1));
    }
    return 0; 
  }
  
  /**
   * Assignment from SELECT statement 
   */
  public Integer assignFromSelect(HplsqlParser.Assignment_stmt_select_itemContext ctx) { 
    String sql = evalPop(ctx.select_stmt()).toString();
    if (trace) {
      trace(ctx, sql.toString());
    }
    String conn = exec.getStatementConnection();
    Query query = exec.executeQuery(ctx, sql.toString(), conn);
    if (query.error()) { 
      exec.signal(query);
      return 1;
    }
    exec.setSqlSuccess();
    try {
      ResultSet rs = query.getResultSet();
      ResultSetMetaData rm = null;
      if (rs != null) {
        rm = rs.getMetaData();
        int cnt = ctx.ident().size();
        if (rs.next()) {
          for (int i = 1; i <= cnt; i++) {
            Var var = exec.findVariable(ctx.ident(i-1).getText());
            if (var != null) {
              var.setValue(rs, rm, i);
              if (trace) {
                trace(ctx, "COLUMN: " + rm.getColumnName(i) + ", " + rm.getColumnTypeName(i));
                trace(ctx, "SET " + var.getName() + " = " + var.toString());
              }             
            } 
            else if(trace) {
              trace(ctx, "Variable not found: " + ctx.ident(i-1).getText());
            }
          }
          exec.incRowCount();
          exec.setSqlSuccess();
        }
        else {
          exec.setSqlCode(100);
          exec.signal(Signal.Type.NOTFOUND);
        }
      }
    }
    catch (SQLException e) {
      exec.signal(query);
      return 1;
    }
    finally {
      exec.closeQuery(query, conn);
    }
    return 0; 
  }
  
  /**
   * SQL INSERT statement
   */
  public Integer insert(HplsqlParser.Insert_stmtContext ctx) {
    exec.stmtConnList.clear();
    if (ctx.select_stmt() != null) {
      return insertSelect(ctx);
    }
    return insertValues(ctx); 
  }
  
  /**
   * SQL INSERT SELECT statement
   */
  public Integer insertSelect(HplsqlParser.Insert_stmtContext ctx) { 
    trace(ctx, "INSERT SELECT");
    String table = evalPop(ctx.table_name()).toString();
    String select = evalPop(ctx.select_stmt()).toString();
    String sql = "INSERT INTO TABLE " + table + " " + select;    
    trace(ctx, sql);
    Query query = exec.executeSql(ctx, sql, exec.conf.defaultConnection);
    if (query.error()) {
      exec.signal(query);
      return 1;
    }
    exec.setSqlSuccess();
    exec.closeQuery(query, exec.conf.defaultConnection);
    return 0; 
  }
  
  /**
   * SQL INSERT VALUES statement
   */
  public Integer insertValues(HplsqlParser.Insert_stmtContext ctx) { 
    trace(ctx, "INSERT VALUES");
    String table = evalPop(ctx.table_name()).toString();
    String conn = exec.getObjectConnection(ctx.table_name().getText());
    Conn.Type type = exec.getConnectionType(conn); 
    StringBuilder sql = new StringBuilder();
    if (type == Conn.Type.HIVE) {
      sql.append("INSERT INTO TABLE " + table + " ");
      if (conf.insertValues == Conf.InsertValues.NATIVE) {
        sql.append("VALUES\n("); 
      }
    }
    else {
      sql.append("INSERT INTO " + table);
      if (ctx.insert_stmt_cols() != null) {
        sql.append(" " + exec.getFormattedText(ctx.insert_stmt_cols()));
      }
      sql.append(" VALUES\n("); 
    }
    int rows = ctx.insert_stmt_rows().insert_stmt_row().size();
    for (int i = 0; i < rows; i++) {
      HplsqlParser.Insert_stmt_rowContext row =ctx.insert_stmt_rows().insert_stmt_row(i);
      int cols = row.expr().size();
      for (int j = 0; j < cols; j++) {         
        String value = evalPop(row.expr(j)).toSqlString();
        if (j == 0 && type == Conn.Type.HIVE && conf.insertValues == Conf.InsertValues.SELECT ) {
          sql.append("SELECT ");
        }
        sql.append(value);
        if (j + 1 != cols) {
          sql.append(", ");
        }       
      }
      if (type != Conn.Type.HIVE || conf.insertValues == Conf.InsertValues.NATIVE) {
        if (i + 1 == rows) {
          sql.append(")");
        } else {
          sql.append("),\n(");
        } 
      }
      else if (type == Conn.Type.HIVE && conf.insertValues == Conf.InsertValues.SELECT) {
        sql.append(" FROM " + conf.dualTable); 
        if (i + 1 < rows) {
          sql.append("\nUNION ALL\n");
        }
      }      
    }    
    if (trace) {
      trace(ctx, sql.toString());
    }
    Query query = exec.executeSql(ctx, sql.toString(), conn);
    if (query.error()) {
      exec.signal(query);
      return 1;
    }
    exec.setSqlSuccess();
    exec.closeQuery(query, exec.conf.defaultConnection);
    return 0; 
  }
  
  /**
   * GET DIAGNOSTICS EXCEPTION statement
   */
  public Integer getDiagnosticsException(HplsqlParser.Get_diag_stmt_exception_itemContext ctx) {
    trace(ctx, "GET DIAGNOSTICS EXCEPTION");
    Signal signal = exec.signalPeek();
    if (signal == null || (signal != null && signal.type != Signal.Type.SQLEXCEPTION)) {
      signal = exec.currentSignal;
    }
    if (signal != null) {
      exec.setVariable(ctx.ident().getText(), signal.getValue());
    }
    return 0; 
  }
  
  /**
   * GET DIAGNOSTICS ROW_COUNT statement
   */
  public Integer getDiagnosticsRowCount(HplsqlParser.Get_diag_stmt_rowcount_itemContext ctx) {
    trace(ctx, "GET DIAGNOSTICS ROW_COUNT");
    exec.setVariable(ctx.ident().getText(), exec.getRowCount());
    return 0;  
  }
  
  /**
   * USE statement
   */
  public Integer use(HplsqlParser.Use_stmtContext ctx) {
    trace(ctx, "USE");
    return use(ctx, ctx.T_USE().toString() + " " + meta.normalizeIdentifierPart(evalPop(ctx.expr()).toString()));
  }
  
  public Integer use(ParserRuleContext ctx, String sql) {
    if(trace) {
      trace(ctx, "SQL statement: " + sql);
    }    
    Query query = exec.executeSql(ctx, sql, exec.conf.defaultConnection);
    if(query.error()) {
      exec.signal(query);
      return 1;
    }
    exec.setSqlCode(0);
    exec.closeQuery(query, exec.conf.defaultConnection);
    return 0; 
  }
  
  /** 
   * VALUES statement
   */
  public Integer values(HplsqlParser.Values_into_stmtContext ctx) { 
    trace(ctx, "VALUES statement");    
    int cnt = ctx.ident().size();        // Number of variables and assignment expressions
    int ecnt = ctx.expr().size();    
    for (int i = 0; i < cnt; i++) {
      String name = ctx.ident(i).getText();      
      if (i < ecnt) {
        visit(ctx.expr(i));
        Var var = exec.setVariable(name);        
        if (trace) {
          trace(ctx, "SET " + name + " = " + var.toString());      
        } 
      }      
    }    
    return 0; 
  } 
  
  /**
   * WHILE statement
   */
  public Integer while_(HplsqlParser.While_stmtContext ctx) {
    trace(ctx, "WHILE - ENTERED");
    String label = exec.labelPop();
    while (true) {
      if (evalPop(ctx.bool_expr()).isTrue()) {
        exec.enterScope(Scope.Type.LOOP);
        visit(ctx.block());
        exec.leaveScope();        
        if (canContinue(label)) {
          continue;
        }
      }
      break;
    }    
    trace(ctx, "WHILE - LEFT");
    return 0; 
  }
  
  /**
   * FOR cursor statement
   */
  public Integer forCursor(HplsqlParser.For_cursor_stmtContext ctx) { 
    trace(ctx, "FOR CURSOR - ENTERED");
    exec.enterScope(Scope.Type.LOOP);
    String cursor = ctx.L_ID().getText();
    String sql = evalPop(ctx.select_stmt()).toString();   
    trace(ctx, sql);
    Query query = exec.executeQuery(ctx, sql, exec.conf.defaultConnection);
    if (query.error()) { 
      exec.signal(query);
      return 1;
    }
    trace(ctx, "SELECT completed successfully");
    exec.setSqlSuccess();
    try {
      ResultSet rs = query.getResultSet();
      if (rs != null) {
        ResultSetMetaData rm = rs.getMetaData();
        int cols = rm.getColumnCount();
        Row row = new Row();
        for (int i = 1; i <= cols; i++) {
          row.addColumn(rm.getColumnName(i), rm.getColumnTypeName(i));
        }
        Var var = new Var(cursor, row);
        exec.addVariable(var);
        while (rs.next()) {
          var.setValues(rs, rm);
          if (trace) {
            trace(ctx, var, rs, rm, 0);
          }
          visit(ctx.block());
          exec.incRowCount();
        }
      }
    }
    catch (SQLException e) {
      exec.signal(e);
      exec.closeQuery(query, exec.conf.defaultConnection);
      return 1;
    }
    exec.setSqlSuccess();
    exec.closeQuery(query, exec.conf.defaultConnection);
    exec.leaveScope();
    trace(ctx, "FOR CURSOR - LEFT");
    return 0; 
  }
  
  /**
   * FOR (integer range) statement
   */
  public Integer forRange(HplsqlParser.For_range_stmtContext ctx) { 
    trace(ctx, "FOR RANGE - ENTERED");
    int start = evalPop(ctx.expr(0)).intValue();
    int end = evalPop(ctx.expr(1)).intValue();
    int step = evalPop(ctx.expr(2), 1L).intValue();
    exec.enterScope(Scope.Type.LOOP);
    Var index = new Var(ctx.L_ID().getText(), new Long(start));       
    exec.addVariable(index);     
    if (ctx.T_REVERSE() == null) {
      for (int i = start; i <= end; i += step) {
        visit(ctx.block());
        index.increment(new Long(step));
      } 
    } else {
      for (int i = start; i >= end; i -= step) {
        visit(ctx.block());
        index.decrement(new Long(step));
      }    
    }
    exec.leaveScope();
    trace(ctx, "FOR RANGE - LEFT");
    return 0; 
  }  
  
  /**
   * EXEC, EXECUTE and EXECUTE IMMEDIATE statement to execute dynamic SQL or stored procedure
   */
  public Integer exec(HplsqlParser.Exec_stmtContext ctx) { 
    if (execProc(ctx)) {
      return 0;
    }
    trace(ctx, "EXECUTE");
    Var vsql = evalPop(ctx.expr());
    String sql = vsql.toString();
    if (trace) {
      trace(ctx, "SQL statement: " + sql);
    }
    Query query = exec.executeSql(ctx, sql, exec.conf.defaultConnection);
    if (query.error()) {
      exec.signal(query);
      return 1;
    }
    ResultSet rs = query.getResultSet();
    if (rs != null) {
      try {
        ResultSetMetaData rm = rs.getMetaData();
        if (ctx.T_INTO() != null) {
          int cols = ctx.L_ID().size();
          if (rs.next()) {
            for (int i = 0; i < cols; i++) {
              Var var = exec.findVariable(ctx.L_ID(i).getText());
              if (var != null) {
                if (var.type != Var.Type.ROW) {
                  var.setValue(rs, rm, i + 1);
                }
                else {
                  var.setValues(rs, rm);
                }
                if (trace) {
                  trace(ctx, var, rs, rm, i + 1);
                }
              } 
              else if (trace) {
                trace(ctx, "Variable not found: " + ctx.L_ID(i).getText());
              }
            }
            exec.setSqlCode(0);
          }
        }
        // Print the results
        else {
          int cols = rm.getColumnCount();
          while(rs.next()) {
            for(int i = 1; i <= cols; i++) {
              if(i > 1) {
                System.out.print("\t");
              }
              System.out.print(rs.getString(i));
            }
            System.out.println("");
          }
        }
      } 
      catch(SQLException e) {
        exec.setSqlCode(e);
      } 
    }   
    exec.closeQuery(query, exec.conf.defaultConnection);
    return 0; 
  }
  
  /**
   * EXEC to execute a stored procedure
   */
  public Boolean execProc(HplsqlParser.Exec_stmtContext ctx) { 
    String name = evalPop(ctx.expr()).toString();
    if (exec.function.isProc(name)) {
      if (exec.function.execProc(ctx.expr_func_params(), name)) {
        return true;
      }
    }
    return false;
  }
      
  /**
   * EXIT statement (leave the specified loop with a condition)
   */
  public Integer exit(HplsqlParser.Exit_stmtContext ctx) { 
    trace(ctx, "EXIT");
    String label = "";
    if (ctx.L_ID() != null) {
      label = ctx.L_ID().toString();
    }
    if (ctx.T_WHEN() != null) {
      if (evalPop(ctx.bool_expr()).isTrue()) {
        leaveLoop(label);
      }
    } else {
      leaveLoop(label);
    }
    return 0;
  }
  
  /**
   * BREAK statement (leave the innermost loop unconditionally)
   */
  public Integer break_(HplsqlParser.Break_stmtContext ctx) { 
    trace(ctx, "BREAK");
    leaveLoop("");
    return 0;
  }
  
  /**
   * LEAVE statement (leave the specified loop unconditionally)
   */
  public Integer leave(HplsqlParser.Leave_stmtContext ctx) { 
    trace(ctx, "LEAVE");
    String label = "";
    if (ctx.L_ID() != null) {
      label = ctx.L_ID().toString();
    }
    leaveLoop(label);    
    return 0;
  }
  
  /**
   * Leave the specified or innermost loop unconditionally
   */
  public void leaveLoop(String value) { 
    exec.signal(Signal.Type.LEAVE_LOOP, value);
  }
  
  /**
   * UPDATE statement
   */
  public Integer update(HplsqlParser.Update_stmtContext ctx) {
    trace(ctx, "UPDATE");
    String sql = exec.getFormattedText(ctx);
    trace(ctx, sql);
    Query query = exec.executeSql(ctx, sql, exec.conf.defaultConnection);
    if (query.error()) {
      exec.signal(query);
      return 1;
    }
    exec.setSqlSuccess();
    exec.closeQuery(query, exec.conf.defaultConnection);
    return 0;
  }
  
  /**
   * DELETE statement
   */
  public Integer delete(HplsqlParser.Delete_stmtContext ctx) {
    trace(ctx, "DELETE");
    String table = evalPop(ctx.table_name()).toString();
    StringBuilder sql = new StringBuilder();
    sql.append("DELETE FROM ");
    sql.append(table);
    if (ctx.where_clause() != null) {
      boolean oldBuildSql = exec.buildSql; 
      exec.buildSql = true;
      sql.append(" " + evalPop(ctx.where_clause()).toString());
      exec.buildSql = oldBuildSql;
    }
    trace(ctx, sql.toString());
    Query query = exec.executeSql(ctx, sql.toString(), exec.conf.defaultConnection);
    if (query.error()) {
      exec.signal(query);
      return 1;
    }
    exec.setSqlSuccess();
    exec.closeQuery(query, exec.conf.defaultConnection);
    return 0;
  }
  
  /**
   * MERGE statement
   */
  public Integer merge(HplsqlParser.Merge_stmtContext ctx) {
    trace(ctx, "MERGE");
    String sql = exec.getFormattedText(ctx);
    trace(ctx, sql);
    Query query = exec.executeSql(ctx, sql, exec.conf.defaultConnection);
    if (query.error()) {
      exec.signal(query);
      return 1;
    }
    exec.setSqlSuccess();
    exec.closeQuery(query, exec.conf.defaultConnection);
    return 0;
  }
  
  /**
   * PRINT Statement 
   */
  public Integer print(HplsqlParser.Print_stmtContext ctx) { 
    trace(ctx, "PRINT");
    if (ctx.expr() != null) {
      visit(ctx.expr());
      System.out.println(stack.pop().toString());
    }
	  return 0; 
  }
  
  /** 
   * SET current schema 
   */
  public Integer setCurrentSchema(HplsqlParser.Set_current_schema_optionContext ctx) { 
    trace(ctx, "SET CURRENT SCHEMA");
    return use(ctx, "USE " + meta.normalizeIdentifierPart(evalPop(ctx.expr()).toString()));
  }
  
  /**
   * SIGNAL statement
   */
  public Integer signal(HplsqlParser.Signal_stmtContext ctx) {
    trace(ctx, "SIGNAL");
    Signal signal = new Signal(Signal.Type.USERDEFINED, ctx.ident().getText());
    exec.signal(signal);
    return 0; 
  }  
  
  /**
   * RESIGNAL statement
   */
  public Integer resignal(HplsqlParser.Resignal_stmtContext ctx) { 
    trace(ctx, "RESIGNAL");
    if (ctx.T_SQLSTATE() != null) {
      String sqlstate = evalPop(ctx.expr(0)).toString();
      String text = "";
      if (ctx.T_MESSAGE_TEXT() != null) {
        text = evalPop(ctx.expr(1)).toString();
      }
      SQLException exception = new SQLException(text, sqlstate, -1);
      Signal signal = new Signal(Signal.Type.SQLEXCEPTION, text, exception);
      exec.setSqlCode(exception);
      exec.resignal(signal);
    }
    else {
      exec.resignal();
    }
    return 0; 
  }
  
  /**
   * RETURN statement
   */
  public Integer return_(HplsqlParser.Return_stmtContext ctx) {
    trace(ctx, "RETURN");
    if (ctx.expr() != null) {
      eval(ctx.expr());
    }
    exec.signal(Signal.Type.LEAVE_ROUTINE);    
    return 0; 
  }  
  
  /**
   * Check if an exception is raised or EXIT executed, and we should leave the block
   */
  boolean canContinue(String label) {
    Signal signal = exec.signalPeek();
    if (signal != null && signal.type == Signal.Type.SQLEXCEPTION) {
      return false;
    }
    signal = exec.signalPeek();
    if (signal != null && signal.type == Signal.Type.LEAVE_LOOP) {
      if (signal.value == null || signal.value.isEmpty() ||
          (label != null && label.equalsIgnoreCase(signal.value))) {
        exec.signalPop();
      }
      return false;
    }
    return true;     
  }
  
  /**
   * Evaluate the expression and push the value to the stack
   */
  void eval(ParserRuleContext ctx) {
    exec.visit(ctx);
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
   * Evaluate the expression and pop value from the stack
   */
  Var evalPop(ParserRuleContext ctx) {
    visit(ctx);
    if (!exec.stack.isEmpty()) { 
      return exec.stackPop();
    }
    return Var.Empty;
  }
  
  Var evalPop(ParserRuleContext ctx, long def) {
    if (ctx != null) {
      exec.visit(ctx);
      return exec.stackPop();
    }
    return new Var(def);
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
  void trace(ParserRuleContext ctx, String message) {
	  exec.trace(ctx, message);
  }
  
  void trace(ParserRuleContext ctx, Var var, ResultSet rs, ResultSetMetaData rm, int idx) throws SQLException {
    exec.trace(ctx, var, rs, rm, idx);
  }
}
