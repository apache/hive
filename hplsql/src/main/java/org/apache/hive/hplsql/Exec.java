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
 
package org.apache.hive.hplsql;

import java.math.BigDecimal;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.Iterator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.io.FileUtils;
import org.apache.hive.hplsql.Var.Type;
import org.apache.hive.hplsql.functions.*;

/**
 * HPL/SQL script executor
 *
 */
public class Exec extends HplsqlBaseVisitor<Integer> {
  
  public static final String VERSION = "HPL/SQL 0.3.31";
  public static final String ERRORCODE = "ERRORCODE";
  public static final String SQLCODE = "SQLCODE";
  public static final String SQLSTATE = "SQLSTATE";
  public static final String HOSTCODE = "HOSTCODE";
  
  Exec exec = null;
  ParseTree tree = null;
  
  public enum OnError {EXCEPTION, SETERROR, STOP}; 

  // Scopes of execution (code blocks) with own local variables, parameters and exception handlers
  Stack<Scope> scopes = new Stack<Scope>();
  Scope globalScope;
  Scope currentScope;
  
  Stack<Var> stack = new Stack<Var>();
  Stack<String> labels = new Stack<String>();
  Stack<String> callStack = new Stack<String>();
  
  Stack<Signal> signals = new Stack<Signal>();
  Signal currentSignal;
  Scope currentHandlerScope;
  boolean resignal = false;
  
  HashMap<String, String> managedTables = new HashMap<String, String>();
  HashMap<String, String> objectMap = new HashMap<String, String>(); 
  HashMap<String, String> objectConnMap = new HashMap<String, String>();
  HashMap<String, ArrayList<Var>> returnCursors = new HashMap<String, ArrayList<Var>>();
  HashMap<String, Package> packages = new HashMap<String, Package>();
  
  Package currentPackageDecl = null;
  
  public ArrayList<String> stmtConnList = new ArrayList<String>();
      
  Arguments arguments = new Arguments();
  public Conf conf;
  Expression expr;
  Function function;  
  Converter converter;
  Meta meta;
  Select select;
  Stmt stmt;
  Conn conn;  
  
  int rowCount = 0;  
  
  String execString;
  String execFile;  
  String execMain;
  StringBuilder localUdf = new StringBuilder();
  boolean initRoutines = false;
  public boolean buildSql = false;
  public boolean inCallStmt = false;
  boolean udfRegistered = false;
  boolean udfRun = false;
    
  boolean dotHplsqlrcExists = false;
  boolean hplsqlrcExists = false;
  
  boolean trace = false; 
  boolean info = true;
  boolean offline = false;
  
  Exec() {
    exec = this;
  }
  
  Exec(Exec exec) {
    this.exec = exec;
  }

  /** 
   * Set a variable using a value from the parameter or the stack 
   */
  public Var setVariable(String name, Var value) {
    if (value == null || value == Var.Empty) {
      if (exec.stack.empty()) {
        return Var.Empty;
      }
      value = exec.stack.pop();
    }
    if (name.startsWith("hplsql.")) {
      exec.conf.setOption(name, value.toString());
      return Var.Empty;
    }
    Var var = findVariable(name);
    if (var != null) {
      var.cast(value);
    }
    else {
      var = new Var(value);
      var.setName(name);
      if(exec.currentScope != null) {
        exec.currentScope.addVariable(var);
      }
    }    
    return var;
  }
  
  public Var setVariable(String name) {
    return setVariable(name, Var.Empty);
  }
  
  public Var setVariable(String name, String value) {
    return setVariable(name, new Var(value));
  }

  public Var setVariable(String name, int value) {
    return setVariable(name, new Var(new Long(value)));
  }

  /** 
   * Set variable to NULL 
   */
  public Var setVariableToNull(String name) {
    Var var = findVariable(name);
    if (var != null) {
      var.removeValue();
    }
    else {
      var = new Var();
      var.setName(name);
      if(exec.currentScope != null) {
        exec.currentScope.addVariable(var);
      }
    }    
    return var;
  }
  
  /**
   * Add a local variable to the current scope
   */
  public void addVariable(Var var) {
    if (currentPackageDecl != null) {
      currentPackageDecl.addVariable(var);
    }
    else if (exec.currentScope != null) {
      exec.currentScope.addVariable(var);
    }
  }
  
  /**
   * Add a condition handler to the current scope
   */
  public void addHandler(Handler handler) {
    if (exec.currentScope != null) {
      exec.currentScope.addHandler(handler);
    }
  }
  
  /**
   * Add a return cursor visible to procedure callers and clients
   */
  public void addReturnCursor(Var var) {
    String routine = callStackPeek();
    ArrayList<Var> cursors = returnCursors.get(routine);
    if (cursors == null) {
      cursors = new ArrayList<Var>();
      returnCursors.put(routine, cursors);
    }
    cursors.add(var);
  }
  
  /**
   * Get the return cursor defined in the specified procedure
   */
  public Var consumeReturnCursor(String routine) {
    ArrayList<Var> cursors = returnCursors.get(routine.toUpperCase());
    if (cursors == null) {
      return null;
    }
    Var var = cursors.get(0);
    cursors.remove(0);
    return var;
  }
  
  /**
   * Push a value to the stack
   */
  public void stackPush(Var var) {
    exec.stack.push(var);  
  }
  
  /**
   * Push a string value to the stack
   */
  public void stackPush(String val) {
    exec.stack.push(new Var(val));  
  }
  
  public void stackPush(StringBuilder val) {
    stackPush(val.toString());  
  }
  
  /**
   * Push a boolean value to the stack
   */
  public void stackPush(Boolean val) {
    exec.stack.push(new Var(val));  
  }

  /**
   * Select a value from the stack, but not remove
   */
  public Var stackPeek() {
    return exec.stack.peek();  
  }
  
  /**
   * Pop a value from the stack
   */
  public Var stackPop() {
    if (!exec.stack.isEmpty()) {
      return exec.stack.pop();
    }
    return Var.Empty;
  }    
  
  /**
   * Push a value to the call stack
   */
  public void callStackPush(String val) {
    exec.callStack.push(val.toUpperCase());  
  }
  
  /**
   * Select a value from the call stack, but not remove
   */
  public String callStackPeek() {
    if (!exec.callStack.isEmpty()) {
      return exec.callStack.peek();
    }
    return null;
  }
  
  /**
   * Pop a value from the call stack
   */
  public String callStackPop() {
    if (!exec.callStack.isEmpty()) {
      return exec.callStack.pop();
    }
    return null;
  }  
  
  /** 
   * Find an existing variable by name 
   */
  public Var findVariable(String name) {
    Var var = null;
    String name1 = name;
    String name1a = null;
    String name2 = null;
    Scope cur = exec.currentScope;
    Package pack = null;
    Package packCallContext = exec.getPackageCallContext();
    ArrayList<String> qualified = exec.meta.splitIdentifier(name);
    if (qualified != null) {
      name1 = qualified.get(0);
      name2 = qualified.get(1);
      pack = findPackage(name1);
      if (pack != null) {        
        var = pack.findVariable(name2);
        if (var != null) {
          return var;
        }
      }
    }    
    if (name1.startsWith(":")) {
      name1a = name1.substring(1);
    }    
    while (cur != null) {
      var = findVariable(cur.vars, name1);
      if (var == null && name1a != null) {
        var = findVariable(cur.vars, name1a);
      }
      if (var == null && packCallContext != null) {
        var = packCallContext.findVariable(name1);
      }
      if (var != null) {
        if (qualified != null) {
          if (var.type == Var.Type.ROW && var.value != null) {
            Row row = (Row)var.value;
            var = row.getValue(name2);
          }
        }
        return var;
      }
      if (cur.type == Scope.Type.ROUTINE) {
        cur = exec.globalScope;
      }
      else {
        cur = cur.parent;
      }
    }    
    return null;
  }
  
  public Var findVariable(Var name) {
    return findVariable(name.getName());
  }
  
  Var findVariable(ArrayList<Var> vars, String name) {
    for (Var var : vars) {
      if (name.equalsIgnoreCase(var.getName())) {
        return var;
      }
    }
    return null;
  }
  
  /**
   * Find a cursor variable by name
   */
  public Var findCursor(String name) {
    Var cursor = exec.findVariable(name);
    if (cursor != null && cursor.type == Type.CURSOR) {
      return cursor;
    }    
    return null;
  }
  
  /**
   * Find the package by name
   */
  Package findPackage(String name) {
    return packages.get(name.toUpperCase());
  }
  
  /**
   * Enter a new scope
   */
  public void enterScope(Scope scope) {
    exec.scopes.push(scope);
  }
  
  public void enterScope(Scope.Type type) {
    enterScope(type, null);
  }
  
  public void enterScope(Scope.Type type, Package pack) {
    exec.currentScope = new Scope(exec.currentScope, type, pack);
    enterScope(exec.currentScope);
  }
  
  void enterGlobalScope() {
    globalScope = new Scope(Scope.Type.GLOBAL);
    currentScope = globalScope;
    enterScope(globalScope);
  }

  /**
   * Leave the current scope
   */
  public void leaveScope() {
    if (!exec.signals.empty()) {
      Scope scope = exec.scopes.peek();
      Signal signal = exec.signals.peek();
      if (exec.conf.onError != OnError.SETERROR) {
        runExitHandler();
      }
      if (signal.type == Signal.Type.LEAVE_ROUTINE && scope.type == Scope.Type.ROUTINE) {
        exec.signals.pop();
      }
    }
    exec.currentScope = exec.scopes.pop().getParent();
  }
  
  /**
   * Send a signal
   */
  public void signal(Signal signal) {
    exec.signals.push(signal);
  }
  
  public void signal(Signal.Type type, String value, Exception exception) {
    signal(new Signal(type, value, exception));
  }
  
  public void signal(Signal.Type type, String value) {
    setSqlCode(-1);
    signal(type, value, null);   
  }
  
  public void signal(Signal.Type type) {
    setSqlCode(-1);
    signal(type, null, null);   
  }
  
  public void signal(Query query) {
    setSqlCode(query.getException());
    signal(Signal.Type.SQLEXCEPTION, query.errorText(), query.getException());
  }
  
  public void signal(Exception exception) {
    setSqlCode(exception);
    signal(Signal.Type.SQLEXCEPTION, exception.getMessage(), exception);
  }
  
  /**
   * Resignal the condition
   */
  public void resignal() {
    resignal(exec.currentSignal);
  }
  
  public void resignal(Signal signal) {
    if (signal != null) {
      exec.resignal = true;
      signal(signal);
    }
  }

  /**
   * Run CONTINUE handlers 
   */
  boolean runContinueHandler() {
    Scope cur = exec.currentScope;    
    exec.currentSignal = exec.signals.pop(); 
    while (cur != null) {
      for (Handler h : cur.handlers) {
        if (h.execType != Handler.ExecType.CONTINUE) {
          continue;
        }
        if ((h.type != Signal.Type.USERDEFINED && h.type == exec.currentSignal.type) ||
            (h.type == Signal.Type.USERDEFINED && h.type == exec.currentSignal.type &&
             h.value.equalsIgnoreCase(exec.currentSignal.value))) {
          trace(h.ctx, "CONTINUE HANDLER");
          enterScope(Scope.Type.HANDLER);
          exec.currentHandlerScope = h.scope; 
          visit(h.ctx.single_block_stmt());
          leaveScope(); 
          exec.currentSignal = null;
          return true;
        }
      }      
      cur = cur.parent;
    } 
    exec.signals.push(exec.currentSignal);
    exec.currentSignal = null;
    return false;
  }
  
  /**
   * Run EXIT handler defined for the current scope 
   */
  boolean runExitHandler() {
    exec.currentSignal = exec.signals.pop();
    for (Handler h : currentScope.handlers) {
      if (h.execType != Handler.ExecType.EXIT) {
        continue;
      }
      if ((h.type != Signal.Type.USERDEFINED && h.type == exec.currentSignal.type) ||
          (h.type == Signal.Type.USERDEFINED && h.type == exec.currentSignal.type &&
           h.value.equalsIgnoreCase(currentSignal.value))) {
        trace(h.ctx, "EXIT HANDLER");
        enterScope(Scope.Type.HANDLER);
        exec.currentHandlerScope = h.scope; 
        visit(h.ctx.single_block_stmt());
        leaveScope(); 
        exec.currentSignal = null;
        return true;
      }        
    }    
    exec.signals.push(exec.currentSignal);
    exec.currentSignal = null;
    return false;
  }
    
  /**
   * Pop the last signal
   */
  public Signal signalPop() {
    if (!exec.signals.empty()) {
      return exec.signals.pop();
    }
    return null;
  }
  
  /**
   * Peek the last signal
   */
  public Signal signalPeek() {
    if (!exec.signals.empty()) {
      return exec.signals.peek();
    }
    return null;
  }
  
  /**
   * Pop the current label
   */
  public String labelPop() {
    if(!exec.labels.empty()) {
      return exec.labels.pop();
    }
    return "";
  }
  
  /**
   * Execute a SQL query (SELECT)
   */
  public Query executeQuery(ParserRuleContext ctx, Query query, String connProfile) {
    if (!exec.offline) {
      exec.rowCount = 0;
      exec.conn.executeQuery(query, connProfile);
      return query;
    }
    setSqlNoData();
    info(ctx, "Not executed - offline mode set");
    return query;
  }

  public Query executeQuery(ParserRuleContext ctx, String sql, String connProfile) {
    return executeQuery(ctx, new Query(sql), connProfile);
  }
  
  /**
   * Prepare a SQL query (SELECT)
   */
  public Query prepareQuery(ParserRuleContext ctx, Query query, String connProfile) {
    if (!exec.offline) {
      exec.rowCount = 0;
      exec.conn.prepareQuery(query, connProfile);
      return query;
    }
    setSqlNoData();
    info(ctx, "Not executed - offline mode set");
    return query;
  }

  public Query prepareQuery(ParserRuleContext ctx, String sql, String connProfile) {
    return prepareQuery(ctx, new Query(sql), connProfile);
  }

  /**
   * Execute a SQL statement 
   */
  public Query executeSql(ParserRuleContext ctx, String sql, String connProfile) {
    if (!exec.offline) {
      exec.rowCount = 0;
      Query query = conn.executeSql(sql, connProfile);
      exec.rowCount = query.getRowCount();
      return query;
    }
    info(ctx, "Not executed - offline mode set");
    return new Query("");
  }  
  
  /**
   * Close the query object
   */
  public void closeQuery(Query query, String conn) {
    if(!exec.offline) {
      exec.conn.closeQuery(query, conn);
    }
  }
  
  /**
   * Register JARs, FILEs and CREATE TEMPORARY FUNCTION for UDF call
   */
  public void registerUdf() {
    if (udfRegistered) {
      return;
    }
    ArrayList<String> sql = new ArrayList<String>();
    String dir = Utils.getExecDir();
    String hplsqlJarName = "hplsql.jar";
    for(String jarName: new java.io.File(dir).list()) {
      if(jarName.startsWith("hive-hplsql") && jarName.endsWith(".jar")) {
        hplsqlJarName = jarName;
        break;
      }
    }
    sql.add("ADD JAR " + dir + hplsqlJarName);
    sql.add("ADD JAR " + dir + "antlr4-runtime-4.5.jar");
    if(!conf.getLocation().equals("")) {
      sql.add("ADD FILE " + conf.getLocation());
    } else {
      sql.add("ADD FILE " + dir + Conf.SITE_XML);
    }
    if (dotHplsqlrcExists) {
      sql.add("ADD FILE " + dir + Conf.DOT_HPLSQLRC);
    }
    if (hplsqlrcExists) {
      sql.add("ADD FILE " + dir + Conf.HPLSQLRC);
    }
    String lu = createLocalUdf();
    if (lu != null) {
      sql.add("ADD FILE " + lu);
    }
    sql.add("CREATE TEMPORARY FUNCTION hplsql AS 'org.apache.hive.hplsql.Udf'");
    exec.conn.addPreSql(exec.conf.defaultConnection, sql);
    udfRegistered = true;
  }

  /**
   * Initialize options
   */
  void initOptions() {
    Iterator<Map.Entry<String,String>> i = exec.conf.iterator();
    while (i.hasNext()) {
      Entry<String,String> item = (Entry<String,String>)i.next();
      String key = (String)item.getKey();
      String value = (String)item.getValue();
      if (key == null || value == null || !key.startsWith("hplsql.")) {
        continue;
      }
      else if (key.compareToIgnoreCase(Conf.CONN_DEFAULT) == 0) {
        exec.conf.defaultConnection = value;
      }
      else if (key.startsWith("hplsql.conn.init.")) {
        exec.conn.addConnectionInit(key.substring(17), value);        
      }
      else if (key.startsWith(Conf.CONN_CONVERT)) {
        exec.conf.setConnectionConvert(key.substring(20), value);        
      }
      else if (key.startsWith("hplsql.conn.")) {
        exec.conn.addConnection(key.substring(12), value);
      }
      else if (key.startsWith("hplsql.")) {
        exec.conf.setOption(key, value);
      }
    }    
  }
  
  /**
   * Set SQLCODE
   */
  public void setSqlCode(int sqlcode) {
    Long code = new Long(sqlcode);
    Var var = findVariable(SQLCODE);
    if (var != null) {
      var.setValue(code);
    }
    var = findVariable(ERRORCODE);
    if (var != null) {
      var.setValue(code);
    }
  }
  
  public void setSqlCode(Exception exception) {
    if (exception instanceof SQLException) {
      setSqlCode(((SQLException)exception).getErrorCode());
      setSqlState(((SQLException)exception).getSQLState());
    }
    else {
      setSqlCode(-1);
      setSqlState("02000");
    }    
  }
  
  /**
   * Set SQLSTATE
   */
  public void setSqlState(String sqlstate) {
    Var var = findVariable(SQLSTATE);
    if (var != null) {
      var.setValue(sqlstate);
    }
  }
    
  /**
   * Set HOSTCODE
   */
  public void setHostCode(int code) {
    Var var = findVariable(HOSTCODE);
    if (var != null) {
      var.setValue(new Long(code));
    }
  }
  
  /**
   * Set successful execution for SQL
   */
  public void setSqlSuccess() {
    setSqlCode(0);
    setSqlState("00000");
  }
  
  /**
   * Set SQL_NO_DATA as the result of SQL execution
   */
  public void setSqlNoData() {
    setSqlCode(100);
    setSqlState("01000");
  }
  
  /**
   * Compile and run HPL/SQL script 
   */
  public Integer run(String[] args) throws Exception {
    enterGlobalScope(); 
    if (init(args) != 0) {
      return 1;
    }
    Var result = run();
    if (result != null) {
      System.out.println(result.toString());
    }
    leaveScope();
    cleanup();
    printExceptions();    
    return getProgramReturnCode();
  }
  
  /**
   * Run already compiled HPL/SQL script (also used from Hive UDF)
   */
  public Var run() {
    if (tree == null) {
      return null;
    }
    if (execMain != null) {
      initRoutines = true;
      visit(tree);
      initRoutines = false;
      exec.function.execProc(execMain);
    }
    else {
      visit(tree);
    }
    if (!exec.stack.isEmpty()) {
      return exec.stackPop();
    }
    return null;
  }
  
  /**
   * Initialize PL/HQL
   */
  Integer init(String[] args) throws Exception {
    if (!parseArguments(args)) {
      return 1;
    }
    // specify the default log4j2 properties file.
    System.setProperty("log4j.configurationFile", "hive-log4j2.properties");
    conf = new Conf();
    conf.init();    
    conn = new Conn(this);
    meta = new Meta(this);
    initOptions();
    
    expr = new Expression(this);
    select = new Select(this);
    stmt = new Stmt(this);
    converter = new Converter(this);
        
    function = new Function(this);
    new FunctionDatetime(this).register(function);
    new FunctionMisc(this).register(function);
    new FunctionString(this).register(function);
    new FunctionOra(this).register(function);
    addVariable(new Var(ERRORCODE, Var.Type.BIGINT, 0L));
    addVariable(new Var(SQLCODE, Var.Type.BIGINT, 0L));
    addVariable(new Var(SQLSTATE, Var.Type.STRING, "00000"));
    addVariable(new Var(HOSTCODE, Var.Type.BIGINT, 0L)); 
    for (Map.Entry<String, String> v : arguments.getVars().entrySet()) {
      addVariable(new Var(v.getKey(), Var.Type.STRING, v.getValue()));
    }    
    InputStream input = null;
    if (execString != null) {
      input = new ByteArrayInputStream(execString.getBytes("UTF-8"));
    }
    else {
      input = new FileInputStream(execFile);
    }
    HplsqlLexer lexer = new HplsqlLexer(new ANTLRInputStream(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    HplsqlParser parser = new HplsqlParser(tokens);
    tree = parser.program();    
    if (trace) {
      System.err.println("Configuration file: " + conf.getLocation());
      System.err.println("Parser tree: " + tree.toStringTree(parser));
    }
    includeRcFile();    
    return 0;
  }
  
  /**
   * Parse command line arguments
   */
  boolean parseArguments(String[] args) {
    boolean parsed = arguments.parse(args);
    if (parsed && arguments.hasVersionOption()) {
      System.err.println(VERSION);
      return false;
    }
    if (!parsed || arguments.hasHelpOption() ||
      (arguments.getExecString() == null && arguments.getFileName() == null)) {
      arguments.printHelp();
      return false;
    }    
    execString = arguments.getExecString();
    execFile = arguments.getFileName();
    execMain = arguments.getMain();    
    if (arguments.hasTraceOption()) {
      trace = true;
    }
    if (arguments.hasOfflineOption()) {
      offline = true;
    }
    if (execString != null && execFile != null) {
      System.err.println("The '-e' and '-f' options cannot be specified simultaneously.");
      return false;
    }   
    return true;
  }
  
  /**
   * Include statements from .hplsqlrc and hplsql rc files
   */
  void includeRcFile() {
    if (includeFile(Conf.DOT_HPLSQLRC, false)) {
      dotHplsqlrcExists = true;
    }
    else {
      if (includeFile(Conf.HPLSQLRC, false)) {
        hplsqlrcExists = true;
      }
    }
    if (udfRun) {
      includeFile(Conf.HPLSQL_LOCALS_SQL, true);
    }
  }
  
  /**
   * Include statements from a file
   */
  boolean includeFile(String file, boolean showError) {
    try {
      String content = FileUtils.readFileToString(new java.io.File(file), "UTF-8");
      if (content != null && !content.isEmpty()) {
        if (trace) {
          trace(null, "INCLUDE CONTENT " + file + " (non-empty)");
        }
        new Exec(this).include(content);
        return true;
      }
    } 
    catch (Exception e) {
      if (showError) {
        error(null, "INCLUDE file error: " + e.getMessage());
      }
    } 
    return false;
  }
  
  /**
   * Execute statements from an include file
   */
  void include(String content) throws Exception {
    InputStream input = new ByteArrayInputStream(content.getBytes("UTF-8"));
    HplsqlLexer lexer = new HplsqlLexer(new ANTLRInputStream(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    HplsqlParser parser = new HplsqlParser(tokens);
    ParseTree tree = parser.program(); 
    visit(tree);    
  }
  
  /**
   * Start executing HPL/SQL script
   */
  @Override 
  public Integer visitProgram(HplsqlParser.ProgramContext ctx) {
    Integer rc = visitChildren(ctx);
    return rc;
  }
  
  /**
   * Enter BEGIN-END block
   */
  @Override  
  public Integer visitBegin_end_block(HplsqlParser.Begin_end_blockContext ctx) { 
    enterScope(Scope.Type.BEGIN_END);
    Integer rc = visitChildren(ctx); 
    leaveScope();
    return rc;
  }
  
  /**
   * Free resources before exit
   */
  void cleanup() {
    for (Map.Entry<String, String> i : managedTables.entrySet()) {
      String sql = "DROP TABLE IF EXISTS " + i.getValue();
      Query query = executeSql(null, sql, exec.conf.defaultConnection);      
      closeQuery(query, exec.conf.defaultConnection);
      if (trace) {
        trace(null, sql);        
      }      
    }
  }
  
  /**
   * Output information about unhandled exceptions
   */
  void printExceptions() {
    while (!signals.empty()) {
      Signal sig = signals.pop();
      if (sig.type == Signal.Type.SQLEXCEPTION) {
        System.err.println("Unhandled exception in HPL/SQL");
      }
      if (sig.exception != null) {
        sig.exception.printStackTrace(); 
      }
      else if (sig.value != null) {
        System.err.println(sig.value);
      }
    }
  } 
  
  /**
   * Get the program return code
   */
  Integer getProgramReturnCode() {
    Integer rc = 0;
    if (!signals.empty()) {
      Signal sig = signals.pop();
      if ((sig.type == Signal.Type.LEAVE_PROGRAM || sig.type == Signal.Type.LEAVE_ROUTINE) && 
        sig.value != null) {
        try {
          rc = Integer.parseInt(sig.value);
        }
        catch(NumberFormatException e) {
          rc = 1;
        }
      }
    }
    return rc;
  }

  /**
   * Executing a statement
   */
  @Override 
  public Integer visitStmt(HplsqlParser.StmtContext ctx) {
    if (ctx.semicolon_stmt() != null) {
      return 0;
    }
    if (initRoutines && ctx.create_procedure_stmt() == null && ctx.create_function_stmt() == null) {
      return 0;
    }
    if (exec.resignal) {
      if (exec.currentScope != exec.currentHandlerScope.parent) {
        return 0;
      }
      exec.resignal = false;
    }
    if (!exec.signals.empty() && exec.conf.onError != OnError.SETERROR) {
      if (!runContinueHandler()) {
        return 0;
      }
    }
    Var prev = stackPop();
    if (prev != null && prev.value != null) {
      System.out.println(prev.toString());
    }
    return visitChildren(ctx); 
  }
  
  /**
   * Executing or building SELECT statement
   */
  @Override 
  public Integer visitSelect_stmt(HplsqlParser.Select_stmtContext ctx) { 
    return exec.select.select(ctx);
  }
  
  @Override 
  public Integer visitCte_select_stmt(HplsqlParser.Cte_select_stmtContext ctx) { 
    return exec.select.cte(ctx); 
  }

  @Override 
  public Integer visitFullselect_stmt(HplsqlParser.Fullselect_stmtContext ctx) { 
    return exec.select.fullselect(ctx);
  }
  
  @Override 
  public Integer visitSubselect_stmt(HplsqlParser.Subselect_stmtContext ctx) { 
    return exec.select.subselect(ctx);
  }  
  
  @Override 
  public Integer visitSelect_list(HplsqlParser.Select_listContext ctx) { 
    return exec.select.selectList(ctx); 
  }
  
  @Override 
  public Integer visitFrom_clause(HplsqlParser.From_clauseContext ctx) { 
    return exec.select.from(ctx); 
  }
  
  @Override 
  public Integer visitFrom_table_name_clause(HplsqlParser.From_table_name_clauseContext ctx) { 
    return exec.select.fromTable(ctx); 
  }
  
  @Override 
  public Integer visitFrom_subselect_clause(HplsqlParser.From_subselect_clauseContext ctx) { 
    return exec.select.fromSubselect(ctx); 
  }
  
  @Override 
  public Integer visitFrom_join_clause(HplsqlParser.From_join_clauseContext ctx) { 
    return exec.select.fromJoin(ctx); 
  }
  
  @Override 
  public Integer visitFrom_table_values_clause(HplsqlParser.From_table_values_clauseContext ctx) { 
    return exec.select.fromTableValues(ctx); 
  }
  
  @Override 
  public Integer visitWhere_clause(HplsqlParser.Where_clauseContext ctx) { 
    return exec.select.where(ctx); 
  }  
  
  @Override 
  public Integer visitSelect_options_item(HplsqlParser.Select_options_itemContext ctx) { 
    return exec.select.option(ctx); 
  }

  /**
   * Column name
   */
  @Override 
  public Integer visitColumn_name(HplsqlParser.Column_nameContext ctx) {
    stackPush(meta.normalizeIdentifierPart(ctx.getText()));
    return 0; 
  }
  
  /**
   * Table name
   */
  @Override 
  public Integer visitTable_name(HplsqlParser.Table_nameContext ctx) {
    String name = ctx.getText();
    String nameUp = name.toUpperCase();
    String nameNorm = meta.normalizeObjectIdentifier(name);
    String actualName = exec.managedTables.get(nameUp);
    String conn = exec.objectConnMap.get(nameUp);
    if (conn == null) {
      conn = conf.defaultConnection;
    }
    stmtConnList.add(conn);    
    if (actualName != null) {
      stackPush(actualName);
      return 0;
    }
    actualName = exec.objectMap.get(nameUp);
    if (actualName != null) {
      stackPush(actualName);
      return 0;
    }
    stackPush(nameNorm);
    return 0; 
  }

  /**
   * SQL INSERT statement
   */
  @Override 
  public Integer visitInsert_stmt(HplsqlParser.Insert_stmtContext ctx) { 
    return exec.stmt.insert(ctx); 
  }
  
  /**
   * INSERT DIRECTORY statement
   */
  @Override 
  public Integer visitInsert_directory_stmt(HplsqlParser.Insert_directory_stmtContext ctx) { 
    return exec.stmt.insertDirectory(ctx); 
  }
    
  /**
   * EXCEPTION block
   */
  @Override 
  public Integer visitException_block_item(HplsqlParser.Exception_block_itemContext ctx) { 
    if (exec.signals.empty()) {
      return 0;
    }
    if (exec.conf.onError == OnError.SETERROR || exec.conf.onError == OnError.STOP) {
      exec.signals.pop();
      return 0;
    }
    if (ctx.L_ID().toString().equalsIgnoreCase("OTHERS")) {
      trace(ctx, "EXCEPTION HANDLER");
      exec.signals.pop();
      enterScope(Scope.Type.HANDLER);
      visit(ctx.block());
      leaveScope(); 
    }
    return 0;
  }
    
  /**
   * DECLARE variable statement
   */
  @Override
  public Integer visitDeclare_var_item(HplsqlParser.Declare_var_itemContext ctx) { 
    String type = null;
    Row row = null;
    String len = null;
    String scale = null;
    Var default_ = null;
    if (ctx.dtype().T_ROWTYPE() != null) {
      row = meta.getRowDataType(ctx, exec.conf.defaultConnection, ctx.dtype().ident().getText());
      if (row == null) {
        type = Var.DERIVED_ROWTYPE;
      }
    }
    else {
      type = getDataType(ctx);
      if (ctx.dtype_len() != null) {
        len = ctx.dtype_len().L_INT(0).getText();
        if (ctx.dtype_len().L_INT(1) != null) {
          scale = ctx.dtype_len().L_INT(1).getText();
        }
      }    
      if (ctx.dtype_default() != null) {
        default_ = evalPop(ctx.dtype_default());
      }
    }
	  int cnt = ctx.ident().size();        // Number of variables declared with the same data type and default
	  for (int i = 0; i < cnt; i++) {  	    
	    String name = ctx.ident(i).getText();
	    if (row == null) {
	      Var var = new Var(name, type, len, scale, default_);	     
	      exec.addVariable(var);		
	      if (ctx.T_CONSTANT() != null) {
	        var.setConstant(true);
	      }
	      if (trace) {
	        if (default_ != null) {
	          trace(ctx, "DECLARE " + name + " " + type + " = " + var.toSqlString());
	        }
	        else {
	          trace(ctx, "DECLARE " + name + " " + type);
	        }
	      }
	    }
	    else {
	      exec.addVariable(new Var(name, row));
	      if (trace) {
          trace(ctx, "DECLARE " + name + " " + ctx.dtype().getText());
        }
	    }
	  }	
	  return 0;
  }
  
  /**
   * Get the variable data type
   */
  String getDataType(HplsqlParser.Declare_var_itemContext ctx) {
    String type = null;
    if (ctx.dtype().T_TYPE() != null) {
      type = meta.getDataType(ctx, exec.conf.defaultConnection, ctx.dtype().ident().getText());
      if (type == null) {
        type = Var.DERIVED_TYPE; 
      }
    }
    else {
      type = getFormattedText(ctx.dtype());
    }
    return type;
  }
  
  /**
   * ALLOCATE CURSOR statement
   */
  @Override 
  public Integer visitAllocate_cursor_stmt(HplsqlParser.Allocate_cursor_stmtContext ctx) { 
    return exec.stmt.allocateCursor(ctx); 
  }

  /**
   * ASSOCIATE LOCATOR statement
   */
  @Override 
  public Integer visitAssociate_locator_stmt(HplsqlParser.Associate_locator_stmtContext ctx) { 
    return exec.stmt.associateLocator(ctx); 
  }

  /**
   * DECLARE cursor statement
   */
  @Override 
  public Integer visitDeclare_cursor_item(HplsqlParser.Declare_cursor_itemContext ctx) { 
    return exec.stmt.declareCursor(ctx); 
  }
  
  /**
   * DESCRIBE statement
   */
  @Override 
  public Integer visitDescribe_stmt(HplsqlParser.Describe_stmtContext ctx) {
    return exec.stmt.describe(ctx);
  }
  
  /**
   * DROP statement
   */
  @Override 
  public Integer visitDrop_stmt(HplsqlParser.Drop_stmtContext ctx) { 
    return exec.stmt.drop(ctx); 
  }
  
  /**
   * OPEN cursor statement
   */
  @Override 
  public Integer visitOpen_stmt(HplsqlParser.Open_stmtContext ctx) { 
    return exec.stmt.open(ctx); 
  }  
  
  /**
   * FETCH cursor statement
   */
  @Override 
  public Integer visitFetch_stmt(HplsqlParser.Fetch_stmtContext ctx) { 
    return exec.stmt.fetch(ctx);
  }

  /**
   * CLOSE cursor statement
   */
  @Override 
  public Integer visitClose_stmt(HplsqlParser.Close_stmtContext ctx) { 
    return exec.stmt.close(ctx); 
  }
  
  /**
   * CMP statement
   */
  @Override 
  public Integer visitCmp_stmt(HplsqlParser.Cmp_stmtContext ctx) { 
    return new Cmp(exec).run(ctx); 
  }
  
  /**
   * COPY statement
   */
  @Override 
  public Integer visitCopy_stmt(HplsqlParser.Copy_stmtContext ctx) { 
    return new Copy(exec).run(ctx); 
  }
  
  /**
   * COPY FROM FTP statement
   */
  @Override 
  public Integer visitCopy_from_ftp_stmt(HplsqlParser.Copy_from_ftp_stmtContext ctx) { 
    return new Ftp(exec).run(ctx); 
  }
  
  /**
   * COPY FROM LOCAL statement
   */
  @Override 
  public Integer visitCopy_from_local_stmt(HplsqlParser.Copy_from_local_stmtContext ctx) { 
    return new Copy(exec).runFromLocal(ctx); 
  }
  
  /**
   * DECLARE HANDLER statement
   */
  @Override 
  public Integer visitDeclare_handler_item(HplsqlParser.Declare_handler_itemContext ctx) {
    trace(ctx, "DECLARE HANDLER");
    Handler.ExecType execType = Handler.ExecType.EXIT;
    Signal.Type type = Signal.Type.SQLEXCEPTION;
    String value = null;
    if (ctx.T_CONTINUE() != null) {
      execType = Handler.ExecType.CONTINUE;
    }    
    if (ctx.ident() != null) {
      type = Signal.Type.USERDEFINED;
      value = ctx.ident().getText();
    }
    else if (ctx.T_NOT() != null && ctx.T_FOUND() != null) {
      type = Signal.Type.NOTFOUND;
    }
    addHandler(new Handler(execType, type, value, exec.currentScope, ctx));
    return 0; 
  }
  
  /**
   * DECLARE CONDITION
   */
  @Override 
  public Integer visitDeclare_condition_item(HplsqlParser.Declare_condition_itemContext ctx) { 
    return 0; 
  }
  
  /**
   * DECLARE TEMPORARY TABLE statement 
   */
  @Override 
  public Integer visitDeclare_temporary_table_item(HplsqlParser.Declare_temporary_table_itemContext ctx) { 
    return exec.stmt.declareTemporaryTable(ctx); 
  }
  
  /**
   * CREATE TABLE statement
   */
  @Override 
  public Integer visitCreate_table_stmt(HplsqlParser.Create_table_stmtContext ctx) { 
    return exec.stmt.createTable(ctx); 
  } 
  
  @Override 
  public Integer visitCreate_table_options_hive_item(HplsqlParser.Create_table_options_hive_itemContext ctx) { 
    return exec.stmt.createTableHiveOptions(ctx); 
  }
  
  @Override 
  public Integer visitCreate_table_options_ora_item(HplsqlParser.Create_table_options_ora_itemContext ctx) { 
    return 0; 
  }
  
  @Override 
  public Integer visitCreate_table_options_td_item(HplsqlParser.Create_table_options_td_itemContext ctx) { 
    return 0; 
  }
  
  @Override 
  public Integer visitCreate_table_options_mssql_item(HplsqlParser.Create_table_options_mssql_itemContext ctx) { 
    return 0; 
  }
  
  @Override 
  public Integer visitCreate_table_options_db2_item(HplsqlParser.Create_table_options_db2_itemContext ctx) { 
    return 0; 
  }
  
  @Override 
  public Integer visitCreate_table_options_mysql_item(HplsqlParser.Create_table_options_mysql_itemContext ctx) { 
    return exec.stmt.createTableMysqlOptions(ctx); 
  }
  
  /**
   * CREATE LOCAL TEMPORARY | VOLATILE TABLE statement 
   */
  @Override 
  public Integer visitCreate_local_temp_table_stmt(HplsqlParser.Create_local_temp_table_stmtContext ctx) { 
    return exec.stmt.createLocalTemporaryTable(ctx); 
  }
  
  /**
   * ALTER TABLE statement
   */
  @Override 
  public Integer visitAlter_table_stmt(HplsqlParser.Alter_table_stmtContext ctx) { 
    return 0; 
  } 
  
  /**
   * CREATE DATABASE | SCHEMA statement
   */
  @Override 
  public Integer visitCreate_database_stmt(HplsqlParser.Create_database_stmtContext ctx) {
    return exec.stmt.createDatabase(ctx);
  }
  
  /**
   * CREATE FUNCTION statement
   */
  @Override 
  public Integer visitCreate_function_stmt(HplsqlParser.Create_function_stmtContext ctx) {
    exec.function.addUserFunction(ctx);
    addLocalUdf(ctx);
    return 0; 
  }

  /**
   * CREATE PACKAGE specification statement
   */
  @Override 
  public Integer visitCreate_package_stmt(HplsqlParser.Create_package_stmtContext ctx) { 
    String name = ctx.ident(0).getText().toUpperCase();
    exec.currentPackageDecl = new Package(name, exec);    
    exec.packages.put(name, exec.currentPackageDecl);
    trace(ctx, "CREATE PACKAGE");
    exec.currentPackageDecl.createSpecification(ctx);
    exec.currentPackageDecl = null;
    return 0; 
  }

  /**
   * CREATE PACKAGE body statement
   */
  @Override 
  public Integer visitCreate_package_body_stmt(HplsqlParser.Create_package_body_stmtContext ctx) { 
    String name = ctx.ident(0).getText().toUpperCase();
    exec.currentPackageDecl = exec.packages.get(name);
    if (exec.currentPackageDecl == null) {
      exec.currentPackageDecl = new Package(name, exec);
      exec.currentPackageDecl.setAllMembersPublic(true);
      exec.packages.put(name, exec.currentPackageDecl);
    }
    trace(ctx, "CREATE PACKAGE BODY");
    exec.currentPackageDecl.createBody(ctx);
    exec.currentPackageDecl = null;
    return 0; 
  }

  /**
   * CREATE PROCEDURE statement
   */
  @Override 
  public Integer visitCreate_procedure_stmt(HplsqlParser.Create_procedure_stmtContext ctx) {
    exec.function.addUserProcedure(ctx);
    addLocalUdf(ctx);                      // Add procedures as they can be invoked by functions
    return 0; 
  }
  
  /**
   * CREATE INDEX statement
   */
  @Override 
  public Integer visitCreate_index_stmt(HplsqlParser.Create_index_stmtContext ctx) { 
    return 0; 
  }
  
  /**
   * Add functions and procedures defined in the current script
   */
  void addLocalUdf(ParserRuleContext ctx) {
    if (exec == this) {                              
      localUdf.append(exec.getFormattedText(ctx));
      localUdf.append("\n");
    }
  }
  
  /**
   * Save local functions and procedures to a file (will be added to the distributed cache) 
   */
  String createLocalUdf() {
    if(localUdf.length() == 0) {
      return null;
    }
    try {
      String file = System.getProperty("user.dir") + "/" + Conf.HPLSQL_LOCALS_SQL; 
      PrintWriter writer = new PrintWriter(file, "UTF-8");
      writer.print(localUdf);
      writer.close();
      return file;
    } 
    catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
      
  /**
   * Assignment statement for single value
   */
  @Override 
  public Integer visitAssignment_stmt_single_item(HplsqlParser.Assignment_stmt_single_itemContext ctx) { 
    String name = ctx.ident().getText();
    visit(ctx.expr());    
    Var var = setVariable(name);
    if (trace) {
      trace(ctx, "SET " + name + " = " + var.toSqlString());      
    }    
    return 0;
  }  

  /**
   * Assignment statement for multiple values
   */
  @Override 
  public Integer visitAssignment_stmt_multiple_item(HplsqlParser.Assignment_stmt_multiple_itemContext ctx) { 
    int cnt = ctx.ident().size();
    int ecnt = ctx.expr().size();    
    for (int i = 0; i < cnt; i++) {
      String name = ctx.ident(i).getText();      
      if (i < ecnt) {
        visit(ctx.expr(i));
        Var var = setVariable(name);        
        if (trace) {
          trace(ctx, "SET " + name + " = " + var.toString());      
        } 
      }      
    }    
    return 0; 
  }
  
  /**
   * Assignment from SELECT statement 
   */
  @Override 
  public Integer visitAssignment_stmt_select_item(HplsqlParser.Assignment_stmt_select_itemContext ctx) { 
    return stmt.assignFromSelect(ctx); 
  }
  
  /**
   * Evaluate an expression
   */
  @Override 
  public Integer visitExpr(HplsqlParser.ExprContext ctx) { 
    if (exec.buildSql) {
      exec.expr.execSql(ctx);
    }
    else {
      exec.expr.exec(ctx);
    }
    return 0;
  }

  /**
   * Evaluate a boolean expression
   */
  @Override 
  public Integer visitBool_expr(HplsqlParser.Bool_exprContext ctx) {
    if (exec.buildSql) {
      exec.expr.execBoolSql(ctx);
    }
    else {
      exec.expr.execBool(ctx);
    }
    return 0; 
  }
  
  @Override 
  public Integer visitBool_expr_binary(HplsqlParser.Bool_expr_binaryContext ctx) {
    if (exec.buildSql) {
      exec.expr.execBoolBinarySql(ctx);
    }
    else {
      exec.expr.execBoolBinary(ctx);
    }
    return 0; 
  }
  
  @Override 
  public Integer visitBool_expr_unary(HplsqlParser.Bool_expr_unaryContext ctx) {
    if (exec.buildSql) {
      exec.expr.execBoolUnarySql(ctx);
    }
    else {
      exec.expr.execBoolUnary(ctx);
    }
    return 0; 
  }
   
  /**
   * Static SELECT statement (i.e. unquoted) or expression
   */
  @Override 
  public Integer visitExpr_select(HplsqlParser.Expr_selectContext ctx) {
    if (ctx.select_stmt() != null) {
      stackPush(new Var(evalPop(ctx.select_stmt())));
    }
    else {
      visit(ctx.expr());
    }
    return 0; 
  }
  
  /**
   * File path (unquoted) or expression
   */
  @Override 
  public Integer visitExpr_file(HplsqlParser.Expr_fileContext ctx) {
    if (ctx.file_name() != null) {
      stackPush(new Var(ctx.file_name().getText()));
    }
    else {
      visit(ctx.expr());
    }
    return 0; 
  }
  
  /**
   * Cursor attribute %ISOPEN, %FOUND and %NOTFOUND
   */
  @Override 
  public Integer visitExpr_cursor_attribute(HplsqlParser.Expr_cursor_attributeContext ctx) {
    exec.expr.execCursorAttribute(ctx);
    return 0; 
  }
    
  /**
   * Function call
   */
  @Override 
  public Integer visitExpr_func(HplsqlParser.Expr_funcContext ctx) {
    String name = ctx.ident().getText();  
    if (exec.buildSql) {
      exec.function.execSql(name, ctx.expr_func_params());
    }
    else {
      Package packCallContext = exec.getPackageCallContext();
      ArrayList<String> qualified = exec.meta.splitIdentifier(name);
      boolean executed = false;
      if (qualified != null) {
        Package pack = findPackage(qualified.get(0));
        if (pack != null) {        
          executed = pack.execFunc(qualified.get(1), ctx.expr_func_params());
        }
      }
      if (!executed && packCallContext != null) {
        executed = packCallContext.execFunc(name, ctx.expr_func_params());
      }
      if (!executed) {        
        exec.function.exec(name, ctx.expr_func_params());
      }
    }
    return 0;
  }
  
  /**
   * Aggregate or window function call
   */
  @Override 
  public Integer visitExpr_agg_window_func(HplsqlParser.Expr_agg_window_funcContext ctx) {
    exec.function.execAggWindowSql(ctx);
    return 0; 
  }
  
  /**
   * Function with specific syntax
   */
  @Override 
  public Integer visitExpr_spec_func(HplsqlParser.Expr_spec_funcContext ctx) { 
    if (exec.buildSql) {
      exec.function.specExecSql(ctx);
    }
    else {
      exec.function.specExec(ctx);
    }
    return 0;
  }  
  
  /**
   * INCLUDE statement
   */
  @Override 
  public Integer visitInclude_stmt(@NotNull HplsqlParser.Include_stmtContext ctx) {
    return exec.stmt.include(ctx); 
  }
    
  /**
   * IF statement (PL/SQL syntax)
   */
  @Override 
  public Integer visitIf_plsql_stmt(HplsqlParser.If_plsql_stmtContext ctx) { 
    return exec.stmt.ifPlsql(ctx); 
  }

  /**
   * IF statement (Transact-SQL syntax)
   */
  @Override  
  public Integer visitIf_tsql_stmt(HplsqlParser.If_tsql_stmtContext ctx) { 
    return exec.stmt.ifTsql(ctx); 
  }
  
  /**
   * IF statement (BTEQ syntax)
   */
  @Override  
  public Integer visitIf_bteq_stmt(HplsqlParser.If_bteq_stmtContext ctx) { 
    return exec.stmt.ifBteq(ctx); 
  }
  
  /**
   * USE statement
   */
  @Override 
  public Integer visitUse_stmt(HplsqlParser.Use_stmtContext ctx) { 
    return exec.stmt.use(ctx); 
  }
  
  /** 
   * VALUES statement
   */
  @Override 
  public Integer visitValues_into_stmt(HplsqlParser.Values_into_stmtContext ctx) { 
    return exec.stmt.values(ctx); 
  }  
  
  /**
   * WHILE statement
   */
  @Override 
  public Integer visitWhile_stmt(HplsqlParser.While_stmtContext ctx) { 
    return exec.stmt.while_(ctx); 
  }  
 
  /**
   * FOR cursor statement
   */
  @Override 
  public Integer visitFor_cursor_stmt(HplsqlParser.For_cursor_stmtContext ctx) { 
    return exec.stmt.forCursor(ctx); 
  }
  
  /**
   * FOR (integer range) statement
   */
  @Override 
  public Integer visitFor_range_stmt(HplsqlParser.For_range_stmtContext ctx) { 
    return exec.stmt.forRange(ctx); 
  }  

  /**
   * EXEC, EXECUTE and EXECUTE IMMEDIATE statement to execute dynamic SQL
   */
  @Override 
  public Integer visitExec_stmt(HplsqlParser.Exec_stmtContext ctx) { 
    exec.inCallStmt = true;
    Integer rc = exec.stmt.exec(ctx);
    exec.inCallStmt = false;
    return rc;
  }
  
  /**
   * CALL statement
   */
  @Override 
  public Integer visitCall_stmt(HplsqlParser.Call_stmtContext ctx) {
    String name = ctx.ident().getText();
    Package packCallContext = exec.getPackageCallContext();
    ArrayList<String> qualified = exec.meta.splitIdentifier(name);
    exec.inCallStmt = true;    
    boolean executed = false;
    if (qualified != null) {
      Package pack = findPackage(qualified.get(0));
      if (pack != null) {        
        executed = pack.execProc(qualified.get(1), ctx.expr_func_params(), true /*trace error if not exists*/);
      }
    }
    if (!executed && packCallContext != null) {
      executed = packCallContext.execProc(name, ctx.expr_func_params(), false /*trace error if not exists*/);
    }
    if (!executed) {        
      exec.function.execProc(name, ctx.expr_func_params(), ctx);
    }
    exec.inCallStmt = false;
    return 0;
  }
    
  /**
   * EXIT statement (leave the specified loop with a condition)
   */
  @Override 
  public Integer visitExit_stmt(HplsqlParser.Exit_stmtContext ctx) { 
    return exec.stmt.exit(ctx); 
  }

  /**
   * BREAK statement (leave the innermost loop unconditionally)
   */
  @Override 
  public Integer visitBreak_stmt(HplsqlParser.Break_stmtContext ctx) { 
    return exec.stmt.break_(ctx);
  }
  
  /**
   * LEAVE statement (leave the specified loop unconditionally)
   */
  @Override 
  public Integer visitLeave_stmt(HplsqlParser.Leave_stmtContext ctx) { 
    return exec.stmt.leave(ctx); 
  }
      
  /** 
   * PRINT statement 
   */
  @Override 
  public Integer visitPrint_stmt(HplsqlParser.Print_stmtContext ctx) { 
	  return exec.stmt.print(ctx); 
  }
  
  /** 
   * QUIT statement 
   */
  @Override 
  public Integer visitQuit_stmt(HplsqlParser.Quit_stmtContext ctx) { 
    return exec.stmt.quit(ctx); 
  }
  
  /**
   * SIGNAL statement
   */
  @Override 
  public Integer visitSignal_stmt(HplsqlParser.Signal_stmtContext ctx) { 
    return exec.stmt.signal(ctx); 
  }
  
  /**
   * SUMMARY statement
   */
  @Override 
  public Integer visitSummary_stmt(HplsqlParser.Summary_stmtContext ctx) { 
    return exec.stmt.summary(ctx); 
  }  
  
  /**
   * RESIGNAL statement
   */
  @Override 
  public Integer visitResignal_stmt(HplsqlParser.Resignal_stmtContext ctx) {  
    return exec.stmt.resignal(ctx); 
  }
    
  /**
   * RETURN statement
   */
  @Override 
  public Integer visitReturn_stmt(HplsqlParser.Return_stmtContext ctx) {
    return exec.stmt.return_(ctx); 
  }  
  
  /** 
   * SET session options
   */
  @Override 
  public Integer visitSet_current_schema_option(HplsqlParser.Set_current_schema_optionContext ctx) { 
    return exec.stmt.setCurrentSchema(ctx); 
  }
  
  /**
   * TRUNCATE statement
   */
  @Override 
  public Integer visitTruncate_stmt(HplsqlParser.Truncate_stmtContext ctx) { 
    return exec.stmt.truncate(ctx); 
  }
  
  /**
   * MAP OBJECT statement
   */
  @Override 
  public Integer visitMap_object_stmt(HplsqlParser.Map_object_stmtContext ctx) {
    String source = evalPop(ctx.expr(0)).toString();
    String target = null;
    String conn = null;
    if (ctx.T_TO() != null) {
      target = evalPop(ctx.expr(1)).toString();
      exec.objectMap.put(source.toUpperCase(), target);  
    }
    if (ctx.T_AT() != null) {
      if (ctx.T_TO() == null) {
        conn = evalPop(ctx.expr(1)).toString();
      }
      else {
        conn = evalPop(ctx.expr(2)).toString();
      }
      exec.objectConnMap.put(source.toUpperCase(), conn);      
    }
    if (trace) {
      String log = "MAP OBJECT " + source;
      if (target != null) {
        log += " AS " + target;
      }
      if (conn != null) {
        log += " AT " + conn;
      }
      trace(ctx, log);
    }
    return 0; 
  }
  
  /**
   * UPDATE statement
   */
  @Override 
  public Integer visitUpdate_stmt(HplsqlParser.Update_stmtContext ctx) { 
    return stmt.update(ctx); 
  }
  
  /**
   * DELETE statement
   */
  @Override 
  public Integer visitDelete_stmt(HplsqlParser.Delete_stmtContext ctx) { 
    return stmt.delete(ctx); 
  }
  
  /**
   * MERGE statement
   */
  @Override 
  public Integer visitMerge_stmt(HplsqlParser.Merge_stmtContext ctx) { 
    return stmt.merge(ctx); 
  }
    
  /**
   * Run a Hive command line
   */
  @Override 
  public Integer visitHive(@NotNull HplsqlParser.HiveContext ctx) { 
    trace(ctx, "HIVE");      
    ArrayList<String> cmd = new ArrayList<String>();
    cmd.add("hive");    
    Var params = new Var(Var.Type.STRINGLIST, cmd);
    stackPush(params);
    visitChildren(ctx);
    stackPop();    
    try { 
      String[] cmdarr = new String[cmd.size()];
      cmd.toArray(cmdarr);
      if(trace) {
        trace(ctx, "HIVE Parameters: " + Utils.toString(cmdarr, ' '));      
      }     
      if (!offline) {
        Process p = Runtime.getRuntime().exec(cmdarr);      
        new StreamGobbler(p.getInputStream()).start();
        new StreamGobbler(p.getErrorStream()).start(); 
        int rc = p.waitFor();      
        if (trace) {
          trace(ctx, "HIVE Process exit code: " + rc);      
        } 
      }
    } catch (Exception e) {
      setSqlCode(-1);
      signal(Signal.Type.SQLEXCEPTION, e.getMessage(), e);
      return -1;
    }    
    return 0; 
  }
  
  @Override 
  @SuppressWarnings("unchecked")
  public Integer visitHive_item(HplsqlParser.Hive_itemContext ctx) { 
    Var params = stackPeek();
    ArrayList<String> a = (ArrayList<String>)params.value;
    String param = ctx.getChild(1).getText();
    if (param.equals("e")) {
      a.add("-e");
      a.add(evalPop(ctx.expr()).toString());
    }   
    else if (param.equals("f")) {
      a.add("-f");
      a.add(evalPop(ctx.expr()).toString());
    }
    else if (param.equals("hiveconf")) {
      a.add("-hiveconf");
      a.add(ctx.L_ID().toString() + "=" + evalPop(ctx.expr()).toString());
    }
    return 0;
  }
  
  /**
   * Executing OS command
   */
  @Override 
  public Integer visitHost_cmd(HplsqlParser.Host_cmdContext ctx) { 
    trace(ctx, "HOST");      
    execHost(ctx, ctx.start.getInputStream().getText(
        new org.antlr.v4.runtime.misc.Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex())));                
    return 0; 
  }
  
  @Override 
  public Integer visitHost_stmt(HplsqlParser.Host_stmtContext ctx) { 
    trace(ctx, "HOST");      
    execHost(ctx, evalPop(ctx.expr()).toString());                
    return 0; 
  }
  
  public void execHost(ParserRuleContext ctx, String cmd) { 
    try { 
      if (trace) {
        trace(ctx, "HOST Command: " + cmd);      
      } 
      Process p = Runtime.getRuntime().exec(cmd);      
      new StreamGobbler(p.getInputStream()).start();
      new StreamGobbler(p.getErrorStream()).start(); 
      int rc = p.waitFor();      
      if (trace) {
        trace(ctx, "HOST Process exit code: " + rc);      
      }
      setHostCode(rc);
    } catch (Exception e) {
      setHostCode(1);
      signal(Signal.Type.SQLEXCEPTION);
    }        
  }
  
  /**
   * Standalone expression (as a statement)
   */
  @Override 
  public Integer visitExpr_stmt(HplsqlParser.Expr_stmtContext ctx) { 	
    visitChildren(ctx); 
 	  return 0;
  }
  
  /**
   * String concatenation operator
   */
  @Override 
  public Integer visitExpr_concat(HplsqlParser.Expr_concatContext ctx) { 
    if (exec.buildSql) {
      exec.expr.operatorConcatSql(ctx);
    }
    else {
      exec.expr.operatorConcat(ctx);
    }
    return 0;
  }
    
  /**
   * Simple CASE expression
   */
  @Override 
  public Integer visitExpr_case_simple(HplsqlParser.Expr_case_simpleContext ctx) { 
    if (exec.buildSql) {
      exec.expr.execSimpleCaseSql(ctx);
    }
    else {
      exec.expr.execSimpleCase(ctx);
    }
    return 0;
  }
  
  /**
   * Searched CASE expression
   */
  @Override 
  public Integer visitExpr_case_searched(HplsqlParser.Expr_case_searchedContext ctx) { 
    if (exec.buildSql) {
      exec.expr.execSearchedCaseSql(ctx);
    }
    else {
      exec.expr.execSearchedCase(ctx);
    }
    return 0;
  }

  /**
   * GET DIAGNOSTICS EXCEPTION statement
   */
  @Override 
  public Integer visitGet_diag_stmt_exception_item(HplsqlParser.Get_diag_stmt_exception_itemContext ctx) { 
    return exec.stmt.getDiagnosticsException(ctx); 
  }  

  /**
   * GET DIAGNOSTICS ROW_COUNT statement
   */
  @Override 
  public Integer visitGet_diag_stmt_rowcount_item(HplsqlParser.Get_diag_stmt_rowcount_itemContext ctx) { 
    return exec.stmt.getDiagnosticsRowCount(ctx);  
  }
  
  /**
   * GRANT statement
   */
  @Override 
  public Integer visitGrant_stmt(HplsqlParser.Grant_stmtContext ctx) { 
    trace(ctx, "GRANT");
    return 0; 
  }
  
  /**
   * Label
   */
  @Override 
  public Integer visitLabel(HplsqlParser.LabelContext ctx) { 
    if (ctx.L_ID() != null) {
      exec.labels.push(ctx.L_ID().toString());
    }
    else {
      String label = ctx.L_LABEL().getText();
      if (label.endsWith(":")) {
        label = label.substring(0, label.length() - 1);
      }
      exec.labels.push(label);
    }
    return 0;
  }
  
  /**
   * Identifier
   */
  @Override 
  public Integer visitIdent(HplsqlParser.IdentContext ctx) { 
    String ident = ctx.getText();
    Var var = findVariable(ident);
    if (var != null) {
      if (!exec.buildSql) {
        exec.stackPush(var);
      }
      else {
        exec.stackPush(new Var(ident, Var.Type.STRING, var.toSqlString()));
      }
    }
    else {
      if (!exec.buildSql && !exec.inCallStmt && exec.function.isProc(ident) && exec.function.execProc(ident, null, ctx)) {
        return 0;
      }
      else {
        exec.stackPush(new Var(Var.Type.IDENT, ident));
      }
    }
    return 0;
  }  
  
  /** 
   * Single quoted string literal 
   */
  @Override 
  public Integer visitSingle_quotedString(HplsqlParser.Single_quotedStringContext ctx) { 
    if (exec.buildSql) {
      exec.stackPush(ctx.getText());
    }
    else {
      exec.stackPush(Utils.unquoteString(ctx.getText()));
    }
    return 0;
  }
  
  /**
   * Integer literal, signed or unsigned
   */
  @Override 
  public Integer visitInt_number(HplsqlParser.Int_numberContext ctx) {
    exec.stack.push(new Var(new Long(ctx.getText())));  	  
	  return 0; 
  }
 
  /**
   * Interval expression (INTERVAL '1' DAY i.e)
   */
  @Override 
  public Integer visitExpr_interval(HplsqlParser.Expr_intervalContext ctx) {
    int num = evalPop(ctx.expr()).intValue();
    Interval interval = new Interval().set(num, ctx.interval_item().getText());
    stackPush(new Var(interval));
    return 0; 
  }
  
  /**
   * Decimal literal, signed or unsigned
   */
  @Override 
  public Integer visitDec_number(HplsqlParser.Dec_numberContext ctx) {
    stackPush(new Var(new BigDecimal(ctx.getText())));     
    return 0; 
  }
  
  /**
   * Boolean literal
   */
  @Override 
  public Integer visitBool_literal(HplsqlParser.Bool_literalContext ctx) {
    boolean val = true;
    if (ctx.T_FALSE() != null) {
      val = false;
    }
    stackPush(new Var(new Boolean(val)));     
    return 0; 
  }

  /**
   * NULL constant
   */
  @Override 
  public Integer visitNull_const(HplsqlParser.Null_constContext ctx) { 
    stackPush(new Var());     
    return 0;  
  }

  /**
   * DATE 'YYYY-MM-DD' literal
   */
  @Override 
  public Integer visitDate_literal(HplsqlParser.Date_literalContext ctx) { 
    if (!exec.buildSql) {
      String str = evalPop(ctx.string()).toString();
      stackPush(new Var(Var.Type.DATE, Utils.toDate(str)));
    }
    else {
      stackPush(getFormattedText(ctx));
    }
    return 0; 
  }

  /**
   * TIMESTAMP 'YYYY-MM-DD HH:MI:SS.FFF' literal
   */
  @Override 
  public Integer visitTimestamp_literal(HplsqlParser.Timestamp_literalContext ctx) { 
    if (!exec.buildSql) {
      String str = evalPop(ctx.string()).toString();
      int len = str.length();
      int precision = 0;
      if (len > 19 && len <= 29) {
        precision = len - 20;
        if (precision > 3) {
          precision = 3;
        }
      }
      stackPush(new Var(Utils.toTimestamp(str), precision));
    }
    else {
      stackPush(getFormattedText(ctx));
    }
    return 0; 
  }
  
  /**
   * Get the package context within which the current routine is executed
   */
  Package getPackageCallContext() {
    Scope cur = exec.currentScope;
    while (cur != null) {
      if (cur.type == Scope.Type.ROUTINE) {
        return cur.pack;
      }
      cur = cur.parent;
    }    
    return null;
  }
  
  /**
   * Define the connection profile to execute the current statement
   */
  String getStatementConnection() {
    if (exec.stmtConnList.contains(exec.conf.defaultConnection)) {
      return exec.conf.defaultConnection;
    }
    else if (!exec.stmtConnList.isEmpty()) {
      return exec.stmtConnList.get(0);
    }
    return exec.conf.defaultConnection;
  }
  
  /**
   * Define the connection profile for the specified object
   * @return
   */
  String getObjectConnection(String name) {
    String conn = exec.objectConnMap.get(name.toUpperCase());
    if (conn != null) {
      return conn;
    }
    return exec.conf.defaultConnection;
  }
  
  /**
   * Get the connection (open the new connection if not available)
   * @throws Exception 
   */
  Connection getConnection(String conn) throws Exception {
    if (conn == null || conn.equalsIgnoreCase("default")) {
      conn = exec.conf.defaultConnection;
    }
    return exec.conn.getConnection(conn);
  }
  
  /**
   * Return the connection to the pool
   */
  void returnConnection(String name, Connection conn) {
    exec.conn.returnConnection(name, conn);
  }
  
  /**
   * Define the database type by profile name
   */
  Conn.Type getConnectionType(String conn) {
    return exec.conn.getTypeByProfile(conn);
  }
  
  /**
   * Get the current database type
   */
  public Conn.Type getConnectionType() {
    return getConnectionType(exec.conf.defaultConnection);
  }
  
  /** 
   * Add managed temporary table
   */
  public void addManagedTable(String name, String managedName) {
    exec.managedTables.put(name, managedName);
  }
  
  /**
   * Get node text including spaces
   */
  String getText(ParserRuleContext ctx) {
    return ctx.start.getInputStream().getText(new org.antlr.v4.runtime.misc.Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
  }
  
  String getText(ParserRuleContext ctx, Token start, Token stop) {
    return ctx.start.getInputStream().getText(new org.antlr.v4.runtime.misc.Interval(start.getStartIndex(), stop.getStopIndex()));
  }
  
  /**
   * Append the text preserving the formatting (space symbols) between tokens
   */
  void append(StringBuilder str, String appendStr, Token start, Token stop) {
    String spaces = start.getInputStream().getText(new org.antlr.v4.runtime.misc.Interval(start.getStartIndex(), stop.getStopIndex()));
    spaces = spaces.substring(start.getText().length(), spaces.length() - stop.getText().length());
    str.append(spaces);
    str.append(appendStr);
  }
  
  void append(StringBuilder str, TerminalNode start, TerminalNode stop) {
    String text = start.getSymbol().getInputStream().getText(new org.antlr.v4.runtime.misc.Interval(start.getSymbol().getStartIndex(), stop.getSymbol().getStopIndex()));
    str.append(text);
  }
   
  /**
   * Get the first non-null node
   */
  TerminalNode nvl(TerminalNode t1, TerminalNode t2) {
    if (t1 != null) {
      return t1;
    }
    return t2;
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
    visit(ctx);
    if (!exec.stack.isEmpty()) { 
      return stackPop();
    }
    return new Var(def);
  } 
  
  /**
   * Evaluate the data type and length 
   * 
   */
  String evalPop(HplsqlParser.DtypeContext type, HplsqlParser.Dtype_lenContext len) {
    if (isConvert(exec.conf.defaultConnection)) {
      return exec.converter.dataType(type, len);
    }
    return getText(type, type.getStart(), len.getStop());
  }
  
  /**
   * Evaluate the expression to NULL
   */
  void evalNull() {
    stackPush(Var.Null); 
  }
  
  /**
   * Get formatted text between 2 tokens
   */
  public String getFormattedText(ParserRuleContext ctx) {
    return ctx.start.getInputStream().getText(
      new org.antlr.v4.runtime.misc.Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));                
  }
  
  /**
   * Flag whether executed from UDF or not
   */
  void setUdfRun(boolean udfRun) {
    this.udfRun = udfRun;
  }
  
  /**
   * Whether on-the-fly SQL conversion is required for the connection 
   */
  boolean isConvert(String connName) {
    return exec.conf.getConnectionConvert(connName);
  }
  
  /**
   * Increment the row count
   */
  public int incRowCount() {
    return exec.rowCount++;
  }
  
  /**
   * Set the row count
   */
  public void setRowCount(int rowCount) {
    exec.rowCount = rowCount;
  }
  
  /**
   * Trace information
   */
  public void trace(ParserRuleContext ctx, String message) {
		if (!trace) {
		  return;
	  }
		if (ctx != null) {
	    System.out.println("Ln:" + ctx.getStart().getLine() + " " + message);
		}
		else {
		  System.out.println(message);
		}
  }
  
  /**
   * Trace values retrived from the database
   */
  public void trace(ParserRuleContext ctx, Var var, ResultSet rs, ResultSetMetaData rm, int idx) throws SQLException {
    if (var.type != Var.Type.ROW) {
      trace(ctx, "COLUMN: " + rm.getColumnName(idx) + ", " + rm.getColumnTypeName(idx));
      trace(ctx, "SET " + var.getName() + " = " + var.toString());  
    }
    else {
      Row row = (Row)var.value;
      int cnt = row.size();
      for (int j = 1; j <= cnt; j++) {
        Var v = row.getValue(j - 1);
        trace(ctx, "COLUMN: " + rm.getColumnName(j) + ", " + rm.getColumnTypeName(j));
        trace(ctx, "SET " + v.getName() + " = " + v.toString());
      }
    }
  }
  
  /**
   * Informational messages
   */
  public void info(ParserRuleContext ctx, String message) {
    if (!info) {
      return;
    }
    if (ctx != null) {
      System.err.println("Ln:" + ctx.getStart().getLine() + " " + message);
    }
    else {
      System.err.println(message);
    }
  }

  /**
   * Error message
   */
  public void error(ParserRuleContext ctx, String message) {
    if (ctx != null) {
      System.err.println("Ln:" + ctx.getStart().getLine() + " " + message);
    }
    else {
      System.err.println(message);
    }
  }
  
  public Stack<Var> getStack() {
    return exec.stack;
  }
 
  public int getRowCount() {
    return exec.rowCount;
  }

  public Conf getConf() {
    return exec.conf;
  }
  
  public Meta getMeta() {
    return exec.meta;
  }
  
  public boolean getTrace() {
    return exec.trace;
  }
  
  public boolean getInfo() {
    return exec.info;
  }
  
  public boolean getOffline() {
    return exec.offline;
  }
} 
