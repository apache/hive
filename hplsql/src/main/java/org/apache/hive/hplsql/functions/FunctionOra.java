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
      execDbmsOutputPutLine(ctx); }});
    f.map.put("UTL_FILE.FOPEN", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { 
      execUtlFileFopen(ctx); }});
    f.map.put("UTL_FILE.GET_LINE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { 
      execUtlFileGetLine(ctx); }});
    f.map.put("UTL_FILE.PUT_LINE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { 
      execUtlFilePutLine(ctx); }});
    f.map.put("UTL_FILE.PUT", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { 
      execUtlFilePut(ctx); }});
    f.map.put("UTL_FILE.FCLOSE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { 
      execUtlFileFclose(ctx); }});
  }
  
  /**
   * Print a text message
   */
  void execDbmsOutputPutLine(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.expr().size() > 0) {
      visit(ctx.expr(0));
      System.out.println(exec.stackPop().toString());
    }
  }
  
  /**
   * Execute UTL_FILE.FOPEN function
   */
  public void execUtlFileFopen(HplsqlParser.Expr_func_paramsContext ctx) {
    String dir = "";
    String name = "";
    boolean write = true;
    boolean overwrite = false;
    int cnt = ctx.expr().size();    
    // Directory
    if (cnt > 0) {
      dir = evalPop(ctx.expr(0)).toString();
    }    
    // File name
    if (cnt > 1) {
      name = evalPop(ctx.expr(1)).toString();
    }    
    // Mode
    if (cnt >= 2) {
      String mode = evalPop(ctx.expr(2)).toString();
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
  void execUtlFileGetLine(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = ctx.expr().size();
    Var file = null;
    Var str = null;
    StringBuilder out = new StringBuilder();
    
    // File handle
    if(cnt > 0) {
      visit(ctx.expr(0));
      file = exec.stackPop();
    }    
    // String variable
    if(cnt > 1) {
      visit(ctx.expr(1));
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
  public void execUtlFilePutLine(HplsqlParser.Expr_func_paramsContext ctx) {
    execUtlFilePut(ctx, true /*newline*/);  
  }

  /**
   * Execute UTL_FILE.PUT function
   */
  public void execUtlFilePut(HplsqlParser.Expr_func_paramsContext ctx) {
    execUtlFilePut(ctx, false /*newline*/);  
  }
  
  /**
   * Write a string to file
   */
  void execUtlFilePut(HplsqlParser.Expr_func_paramsContext ctx, boolean newline) {
    int cnt = ctx.expr().size();
    Var file = null;
    String str = "";
    
    // File handle
    if(cnt > 0) {
      visit(ctx.expr(0));
      file = exec.stackPop();
    }    
    // Text string
    if(cnt > 1) {
      visit(ctx.expr(1));
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
  void execUtlFileFclose(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = ctx.expr().size();
    Var file = null;
    
    // File handle
    if(cnt > 0) {
      visit(ctx.expr(0));
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
