/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hive.hplsql.udf;

import org.apache.hadoop.hive.metastore.api.StoredProcedure;
import org.apache.hadoop.hive.metastore.api.StoredProcedureRequest;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hive.hplsql.Arguments;
import org.apache.hive.hplsql.Exec;
import org.apache.hive.hplsql.Scope;
import org.apache.hive.hplsql.Var;
import org.apache.hive.hplsql.executor.QueryExecutor;
import org.apache.thrift.TException;

@Description(name = "hplsql", value = "_FUNC_('query' [, :1, :2, ...n]) - Execute HPL/SQL query", extended = "Example:\n" + " > SELECT _FUNC_('CURRENT_DATE') FROM src LIMIT 1;\n")
@UDFType(deterministic = false)
public class Udf extends GenericUDF {
  public static String NAME = "hplsql";
  transient Exec exec;
  StringObjectInspector queryOI;
  ObjectInspector[] argumentsOI;
  private String functionDefinition;

  public Udf() {
  }

  public Udf(Exec exec) {
    this.exec = exec;
  }

  /**
   * Initialize UDF
   */
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length == 0) {
      throw new UDFArgumentLengthException("At least one argument must be specified");
    }
    if (!(arguments[0] instanceof StringObjectInspector)) {
      throw new UDFArgumentException("First argument must be a string");
    }
    SessionState sessionState = SessionState.get();
    if (sessionState != null) {
      // we are still in HiveServer, get the source of the HplSQL function and store it.
      functionDefinition = loadSource(sessionState, functionName(arguments[0]));
    }
    queryOI = (StringObjectInspector)arguments[0];
    argumentsOI = arguments;
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  protected String loadSource(SessionState sessionState, String functionName) throws UDFArgumentException {
    Exec exec = sessionState.getDynamicVar(Exec.class);
    try {
      StoredProcedure storedProcedure = exec.getMsc().getStoredProcedure(
              new StoredProcedureRequest(
                      SessionState.get().getCurrentCatalog(),
                      SessionState.get().getCurrentDatabase(),
                      functionName));
      return storedProcedure != null ? storedProcedure.getSource() : null;
    } catch (TException e) {
      throw new UDFArgumentException(e);
    }
  }

  protected String functionName(ObjectInspector argument) {
    ConstantObjectInspector inspector = (ConstantObjectInspector) (argument);
    String functionCall = inspector.getWritableConstantValue().toString();
    return functionCall.split("\\(")[0].toUpperCase();
  }

  /**
   * Execute UDF
   */
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (exec == null) {
      exec = new Exec();
      exec.setQueryExecutor(QueryExecutor.DISABLED);
      exec.init();
      if (functionDefinition != null) { // if it's null, it can be a built-in function
        exec.parseAndEval(Arguments.script(functionDefinition));
      }
    }
    exec.enterScope(Scope.Type.ROUTINE);
    setParameters(arguments);
    String query = queryOI.getPrimitiveJavaObject(arguments[0].get());
    try {
      Var result = exec.parseAndEval(Arguments.script(query));
      exec.callStackPop();
      return result != null ? result.toString() : null;
    } finally {
      exec.close();
    }
  }

  /**
   * Set parameters for the current call
   */
  void setParameters(DeferredObject[] arguments) throws HiveException {
    for (int i = 1; i < arguments.length; i++) {
      String name = ":" + i;
      if (argumentsOI[i] instanceof StringObjectInspector) {
        String value = ((StringObjectInspector)argumentsOI[i]).getPrimitiveJavaObject(arguments[i].get());
        if (value != null) {
          exec.setVariable(name, value);
        }
      }
      else if (argumentsOI[i] instanceof IntObjectInspector) {
        Integer value = (Integer)((IntObjectInspector)argumentsOI[i]).getPrimitiveJavaObject(arguments[i].get());
        if (value != null) {
          exec.setVariable(name, new Var(new Long(value)));
        }
      }
      else if (argumentsOI[i] instanceof LongObjectInspector) {
        Long value = (Long)((LongObjectInspector)argumentsOI[i]).getPrimitiveJavaObject(arguments[i].get());
        if (value != null) {
          exec.setVariable(name, new Var(value));
        }
      }
      else {
        exec.setVariableToNull(name);
      }
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "hplsql";
  }
}
