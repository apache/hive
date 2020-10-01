/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hive.hplsql.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PosParam;
import org.apache.hadoop.hive.metastore.api.StoredProcedure;
import org.apache.hive.hplsql.Exec;
import org.apache.hive.hplsql.HplsqlLexer;
import org.apache.hive.hplsql.HplsqlParser;
import org.apache.hive.hplsql.Scope;
import org.apache.hive.hplsql.Var;
import org.apache.thrift.TException;

public class HmsFunction implements Function {
  public static final String LANGUAGE = "HPL/SQL";
  private Exec exec;
  private boolean trace;
  private IMetaStoreClient msc;
  private BuiltinFunctions builtinFunctions;
  private Map<String, StoredProcedure> cache = new HashMap<>();

  public HmsFunction(Exec e, IMetaStoreClient msc, BuiltinFunctions builtinFunctions) {
    this.exec = e;
    this.msc = msc;
    this.builtinFunctions = builtinFunctions;
    this.trace = exec.getTrace();
  }

  @Override
  public boolean exists(String name) {
    return cache.containsKey(name) || getProcFromHMS(name).isPresent();
  }

  @Override
  public boolean exec(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    if (builtinFunctions.exec(name, ctx)) {
      return true;
    }
    if (cache.containsKey(name)) {
      trace(ctx, "EXEC CACHED FUNCTION " + name);
      execProc(cache.get(name), ctx);
      return true;
    }
    Optional<StoredProcedure> proc = getProcFromHMS(name);
    if (proc.isPresent()) {
      trace(ctx, "EXEC HMS FUNCTION " + name);
      execProc(proc.get(), ctx);
      cache.put(name, proc.get());
      return true;
    }
    return false;
  }

  /**
   * Execute a stored procedure using CALL or EXEC statement passing parameters
   */
  private void execProc(StoredProcedure proc, HplsqlParser.Expr_func_paramsContext ctx) {
    exec.callStackPush(proc.getName());
    HashMap<String, Var> out = new HashMap<>();
    ArrayList<Var> actualParams = getActualCallParameters(ctx);
    exec.enterScope(Scope.Type.ROUTINE);
    setCallParameters(ctx, actualParams, proc, out);
    evalStoredProcedure(proc);
    exec.callStackPop();
    exec.leaveScope();
    for (Map.Entry<String, Var> i : out.entrySet()) { // Set OUT parameters
      exec.setVariable(i.getKey(), i.getValue());
    }
  }

  private Optional<StoredProcedure> getProcFromHMS(String name) {
    try {
      Database db = currentDatabase();
      return Optional.of(msc.getStoredProcedure(db.getCatalogName(), db.getName(), name));
    } catch (NoSuchObjectException e) {
      return Optional.empty();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private void evalStoredProcedure(StoredProcedure proc) {
    HplsqlLexer lexer = new HplsqlLexer(new ANTLRInputStream(proc.getSource()));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    HplsqlParser parser = new HplsqlParser(tokens);
    exec.visit(parser.program());
  }

  /**
   * Set parameters for user-defined function call
   */
  private void setCallParameters(HplsqlParser.Expr_func_paramsContext actual, ArrayList<Var> actualValues,
                                StoredProcedure procedure,
                                HashMap<String, Var> out) {
    if (actual == null || actual.func_param() == null || actualValues == null) {
      return;
    }
    int actualCnt = actualValues.size();
    int formalCnt = procedure.getPosParamsSize();
    for (int i = 0; i < actualCnt; i++) {
      if (i >= formalCnt) {
        break;
      }
      PosParam formal = procedure.getPosParams().get(i);
      Var var = setCallParameter(
              formal.getName(),
              formal.getType(),
              formal.isSetLength() ? formal.getLength() : null,
              formal.isSetScale() ? formal.getScale() : null,
              actualValues.get(i));
      if (trace) {
        trace(actual, "SET PARAM " + formal.getName() + " = " + var.toString());
      }
      if (formal.isIsOut()) {
        HplsqlParser.ExprContext a = actual.func_param(i).expr();
        String actualName = a.expr_atom().ident().getText();
        if (actualName != null) {
          out.put(actualName, var);
        }
      }
    }
  }

  private Var setCallParameter(String name, String type, Integer len, Integer scale, Var value) {
    Var var = new Var(name, type, len, scale, null);
    var.cast(value);
    exec.addVariable(var);
    return var;
  }

  private ArrayList<Var> getActualCallParameters(HplsqlParser.Expr_func_paramsContext actual) {
    if (actual == null || actual.func_param() == null) {
      return null;
    }
    int cnt = actual.func_param().size();
    ArrayList<Var> values = new ArrayList<>(cnt);
    for (int i = 0; i < cnt; i++) {
      values.add(evalPop(actual.func_param(i).expr()));
    }
    return values;
  }

  @Override
  public void addUserFunction(HplsqlParser.Create_function_stmtContext ctx) {
    String name = ctx.ident().getText();
    if (builtinFunctions.exists(name)) {
      exec.info(ctx, name + " is a built-in function which cannot be redefined.");
      return;
    }
    trace(ctx, "CREATE FUNCTION " + name);
    Database db = currentDatabase();
    StoredProcedure proc = newStoredProc(name, formalParameters(ctx), returnType(ctx), body(ctx), db);
    cache.put(name, proc);
    saveStoredProcInHMS(db, proc);
  }

  private String body(HplsqlParser.Create_function_stmtContext ctx) {
    if (ctx.declare_block_inplace() != null) {
      return Exec.getFormattedText(ctx.declare_block_inplace()) + "\n" + Exec.getFormattedText(ctx.single_block_stmt());
    } else {
      return Exec.getFormattedText(ctx.single_block_stmt());
    }
  }

  private String returnType(HplsqlParser.Create_function_stmtContext ctx) {
    return ctx.create_function_return().dtype().getText();
  }

  @Override
  public void addUserProcedure(HplsqlParser.Create_procedure_stmtContext ctx) {
    String name = ctx.ident(0).getText();
    if (builtinFunctions.exists(name)) {
      exec.info(ctx, name + " is a built-in function which cannot be redefined.");
      return;
    }
    trace(ctx, "CREATE PROCEDURE " + name);
    Database db = currentDatabase();
    StoredProcedure proc = newStoredProc(name, formalParameters(ctx), null, Exec.getFormattedText(ctx.proc_block()), db);
    cache.put(name, proc);
    saveStoredProcInHMS(db, proc);
  }

  private Database currentDatabase() {
    try {
      return msc.getDatabase(exec.getSchema());
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private void saveStoredProcInHMS(Database db, StoredProcedure proc) {
    try {
      msc.createStoredProcedure(db.getCatalogName(), proc);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private StoredProcedure newStoredProc(String name, List<PosParam> formalParams, String returnType, String source, Database db) {
    StoredProcedure storedProcedure = new StoredProcedure();
    storedProcedure.setName(name);
    storedProcedure.setLanguage(LANGUAGE);
    storedProcedure.setOwnerName(db.getOwnerName()); // TODO
    storedProcedure.setDbName(db.getName());
    storedProcedure.setPosParams(formalParams);
    storedProcedure.setReturnType(returnType);
    storedProcedure.setSource(source);
    return storedProcedure;
  }

  private List<PosParam> formalParameters(ParserRuleContext ctx) {
    HplsqlParser.Create_routine_paramsContext formal = formalParamContext(ctx);
    List<PosParam> result = new ArrayList<>();
    for (int i = 0; i < formal.create_routine_param_item().size(); i++) {
      HplsqlParser.Create_routine_param_itemContext param = formal.create_routine_param_item(i);
      result.add(convertToPosParam(param));
    }
    return result;
  }

  private PosParam convertToPosParam(HplsqlParser.Create_routine_param_itemContext param) {
    PosParam posParam = new PosParam(
            param.ident().getText(),
            param.dtype().getText(),
            param.T_OUT() != null || param.T_INOUT() != null);
    setLenScale(param, posParam);
    return posParam;
  }

  private void setLenScale(HplsqlParser.Create_routine_param_itemContext param, PosParam posParam) {
    if (param.dtype_len() != null) {
      String len = param.dtype_len().L_INT(0).getText();
      posParam.setLength(Integer.parseInt(len));
      if (param.dtype_len().L_INT(1) != null) {
        String scale = param.dtype_len().L_INT(1).getText();
        posParam.setScale(Integer.parseInt(scale));
      }
    }
  }

  private HplsqlParser.Create_routine_paramsContext formalParamContext(ParserRuleContext ctx) {
    HplsqlParser.Create_routine_paramsContext formal;
    if (ctx instanceof HplsqlParser.Create_procedure_stmtContext) {
      formal = ((HplsqlParser.Create_procedure_stmtContext) ctx).create_routine_params();
    } else if (ctx instanceof HplsqlParser.Create_function_stmtContext) {
      formal = ((HplsqlParser.Create_function_stmtContext) ctx).create_routine_params();
    } else {
      throw new IllegalArgumentException("Expected function or procedure context");
    }
    return formal;
  }

  /**
   * Evaluate the expression and pop value from the stack
   */
  private Var evalPop(ParserRuleContext ctx) {
    exec.visit(ctx);
    return exec.stackPop();
  }

  private void trace(ParserRuleContext ctx, String message) {
    if (trace) {
      exec.trace(ctx, message);
    }
  }
}
