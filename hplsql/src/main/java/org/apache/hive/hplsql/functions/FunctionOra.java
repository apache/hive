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

import java.io.IOException;
import java.io.EOFException;

import org.apache.hive.hplsql.*;

public class FunctionOra extends Function {
  public FunctionOra(Exec e) {
    super(e);
  }

  /** 
   * Register functions
   */
  @Override
  public void register(Function f) {  
    f.map.put("DBMS_OUTPUT.PUT_LINE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { 
      dbmsOutputPutLine(ctx); }});
    f.map.put("UTL_FILE.FOPEN", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { 
      utlFileFopen(ctx); }});
    f.map.put("UTL_FILE.GET_LINE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { 
      utlFileGetLine(ctx); }});
    f.map.put("UTL_FILE.PUT_LINE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { 
      utlFilePutLine(ctx); }});
    f.map.put("UTL_FILE.PUT", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { 
      utlFilePut(ctx); }});
    f.map.put("UTL_FILE.FCLOSE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { 
      utlFileFclose(ctx); }});
  }
  
  /**
   * Print a text message
   */
  void dbmsOutputPutLine(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() > 0) {
      System.out.println(evalPop(ctx.func_param(0).expr()));
    }
  }
  
  /**
   * Execute UTL_FILE.FOPEN function
   */
  public void utlFileFopen(HplsqlParser.Expr_func_paramsContext ctx) {
    String dir = "";
    String name = "";
    boolean write = true;
    boolean overwrite = false;
    int cnt = ctx.func_param().size();    
    // Directory
    if (cnt > 0) {
      dir = evalPop(ctx.func_param(0).expr()).toString();
    }    
    // File name
    if (cnt > 1) {
      name = evalPop(ctx.func_param(1).expr()).toString();
    }    
    // Mode
    if (cnt >= 2) {
      String mode = evalPop(ctx.func_param(2).expr()).toString();
      if (mode.equalsIgnoreCase("r")) {
        write = false;
      }
      else if (mode.equalsIgnoreCase("w")) {
        write = true;
        overwrite = true;
      }
    }    
    File file = new File();    
    if (write) {
      file.create(dir, name, overwrite);
    }
    else {
      file.open(dir, name);
    }        
    exec.stackPush(new Var(Var.Type.FILE, file));
  }

  /**
   * Read a text line from an open file
   */
  void utlFileGetLine(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = ctx.func_param().size();
    Var file = null;
    Var str = null;
    StringBuilder out = new StringBuilder();    
    // File handle
    if(cnt > 0) {
      visit(ctx.func_param(0).expr());
      file = exec.stackPop();
    }    
    // String variable
    if(cnt > 1) {
      visit(ctx.func_param(1).expr());
      str = exec.stackPop();
    }
    
    if(file != null && file.type == Var.Type.FILE) {
      File f = (File)file.value;
      
      if(trace) {
        trace(ctx, "File: " + f.toString());      
      }
      
      try {
        while(true) {
          char c = f.readChar();
          if(c == '\n') {
            break;
          }
          out.append(c);
        }        
      } catch (IOException e) {
        if(!(e instanceof EOFException)) {
          out.setLength(0);
        }
      }
      
      // Set the new value to the output string variable
      if(str != null) {
        str.setValue(out.toString());
        
        if(trace) {
          trace(ctx, "OUT " + str.getName() + " = " + str.toString());      
        }
      }
    }
    else if(trace) {
      trace(ctx, "Variable of FILE type not found");      
    }
  }
  
  /**
   * Execute UTL_FILE.PUT_LINE function
   */
  public void utlFilePutLine(HplsqlParser.Expr_func_paramsContext ctx) {
    utlFilePut(ctx, true /*newline*/);  
  }

  /**
   * Execute UTL_FILE.PUT function
   */
  public void utlFilePut(HplsqlParser.Expr_func_paramsContext ctx) {
    utlFilePut(ctx, false /*newline*/);  
  }
  
  /**
   * Write a string to file
   */
  void utlFilePut(HplsqlParser.Expr_func_paramsContext ctx, boolean newline) {
    int cnt = ctx.func_param().size();
    Var file = null;
    String str = "";
    
    // File handle
    if(cnt > 0) {
      visit(ctx.func_param(0).expr());
      file = exec.stackPop();
    }    
    // Text string
    if(cnt > 1) {
      visit(ctx.func_param(1).expr());
      str = exec.stackPop().toString();
    }
    
    if(file != null && file.type == Var.Type.FILE) {
      File f = (File)file.value;
      
      if(trace) {
        trace(ctx, "File: " + f.toString());      
      }
      
      f.writeString(str); 
      
      if(newline) {
        f.writeString("\n");
      }
    }
    else if(trace) {
      trace(ctx, "Variable of FILE type not found");      
    }
  }
  
  /**
   * Execute UTL_FILE.FCLOSE function
   */
  void utlFileFclose(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = ctx.func_param().size();
    Var file = null;
    
    // File handle
    if(cnt > 0) {
      visit(ctx.func_param(0).expr());
      file = exec.stackPop();
    }    
        
    if(file != null && file.type == Var.Type.FILE) {
      File f = (File)file.value;
      
      if(trace) {
        trace(ctx, "File: " + f.toString());      
      }
      
      f.close();      
      file.removeValue();
    }
    else if(trace) {
      trace(ctx, "Variable of FILE type not found");      
    }
  }
}
