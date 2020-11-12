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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hive.hplsql.Exec;
import org.apache.hive.hplsql.Scope;
import org.apache.hive.hplsql.Var;

@Description(name = "hplsql", value = "_FUNC_('query' [, :1, :2, ...n]) - Execute HPL/SQL query", extended = "Example:\n" + " > SELECT _FUNC_('CURRENT_DATE') FROM src LIMIT 1;\n")
@UDFType(deterministic = false)
public class Udf extends GenericUDF {

  transient Exec exec;
  StringObjectInspector queryOI;
  ObjectInspector[] argumentsOI;

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
    queryOI = (StringObjectInspector)arguments[0];
    argumentsOI = arguments;
    if (exec == null) {
      exec = SessionState.get() == null ? null : SessionState.get().getDynamicVar(Exec.class);
    }
    if (exec == null) {
      throw new UDFArgumentException("Cannot be used in non HPL/SQL mode.");
    }
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  /**
   * Execute UDF
   */
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String query = queryOI.getPrimitiveJavaObject(arguments[0].get());
    exec.enterScope(Scope.Type.ROUTINE);
    if (arguments.length > 1) {
      setParameters(arguments);
    }
    setParameters(arguments);
    Var result = exec.eval(query);
    exec.callStackPop();
    exec.leaveScope();
    return result != null ? result.toString() : null;
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
