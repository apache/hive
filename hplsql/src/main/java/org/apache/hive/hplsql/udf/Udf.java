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

import org.apache.hadoop.hive.common.type.*;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hive.hplsql.Arguments;
import org.apache.hive.hplsql.Exec;
import org.apache.hive.hplsql.Scope;
import org.apache.hive.hplsql.Var;
import org.apache.hive.hplsql.executor.QueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "hplsql", value = "_FUNC_('query' [, :1, :2, ...n], 'storedProcedure') - Execute HPL/SQL query",
        extended = "Example:\n" + " > SELECT _FUNC_('CURRENT_DATE') FROM src LIMIT 1;\n")
@UDFType(deterministic = false)
public class Udf extends GenericUDF {
  private static final Logger LOG = LoggerFactory.getLogger(Udf.class.getName());
  public static String NAME = "hplsql";
  private final transient Exec exec = new Exec();
  private StringObjectInspector queryOI;
  private ObjectInspector[] argumentsOI;
  private StringObjectInspector funcDefOI;
  private String functionDefinition = null;

  public Udf() {
    exec.setQueryExecutor(QueryExecutor.DISABLED);
    exec.init();
  }

  /**
   * Initialize UDF
   */
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException("At least two arguments must be specified");
    }
    if (!(arguments[0] instanceof StringObjectInspector)) {
      throw new UDFArgumentException("First argument must be a string");
    }
    if (!(arguments[arguments.length-1] instanceof StringObjectInspector)) {
      throw new UDFArgumentException("Last argument (stored procedure) must be a string");
    }
    queryOI = (StringObjectInspector)arguments[0];
    funcDefOI = (StringObjectInspector)arguments[arguments.length-1];
    argumentsOI = arguments;
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  /**
   * Execute UDF
   */
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (functionDefinition == null) { // if it's null, it can be a built-in function
      int idx = arguments.length-1;
      setParameterForPrimitiveTypeArgument(":" + idx, arguments[idx].get(), funcDefOI);
      functionDefinition = funcDefOI.getPrimitiveJavaObject(arguments[idx].get());
      LOG.debug("functionDefinition: {}", functionDefinition);
      exec.parseAndEval(Arguments.script(functionDefinition));
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
   * Getter for Exec object
   *
   * @return Exec
   */
  public Exec getExec() {
    return this.exec;
  }

  /**
   * Set parameters for the current call
   */
  void setParameters(DeferredObject[] arguments) throws HiveException {
    for (int i = 1; i < arguments.length - 1; i++) {
      String name = ":" + i;
      Object inputObject = arguments[i].get();
      ObjectInspector objectInspector = argumentsOI[i];
      if (objectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE) {
        setParameterForPrimitiveTypeArgument(name, inputObject, objectInspector);
      } else {
        exec.setVariableToNull(name);
      }
    }
  }

  private void setParameterForPrimitiveTypeArgument(String name, Object inputObject, ObjectInspector objectInspector) {
    PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
        ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory();
    switch (primitiveCategory) {
    case BOOLEAN:
      Boolean booleanValue = (Boolean) ((BooleanObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (booleanValue != null) {
        exec.setVariable(name, new Var(booleanValue));
      }
      break;
    case SHORT:
      Short shortValue = (Short) ((ShortObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (shortValue != null) {
        exec.setVariable(name, new Var(shortValue.longValue()));
      }
      break;
    case INT:
      Integer intValue = (Integer) ((IntObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (intValue != null) {
        exec.setVariable(name, new Var(intValue.longValue()));
      }
      break;
    case LONG:
      Long longValue = (Long) ((LongObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (longValue != null) {
        exec.setVariable(name, new Var(longValue));
      }
      break;
    case FLOAT:
      Float floatValue = (Float) ((FloatObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (floatValue != null) {
        exec.setVariable(name, new Var(floatValue.doubleValue()));
      }
      break;
    case DOUBLE:
      Double doubleValue = (Double) ((DoubleObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (doubleValue != null) {
        exec.setVariable(name, new Var(doubleValue));
      }
      break;
    case STRING:
      String strValue = ((StringObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (strValue != null) {
        exec.setVariable(name, new Var(strValue));
      }
      break;
    case DATE:
      Date dateValue = ((DateObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (dateValue != null) {
        exec.setVariable(name, new Var(java.sql.Date.valueOf(dateValue.toString())));
      }
      break;
    case TIMESTAMP:
      Timestamp timestampValue = ((TimestampObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (timestampValue != null) {
        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(timestampValue.toString());
        timestamp.setNanos(timestampValue.getNanos());
        exec.setVariable(name, new Var(timestamp, 0));
      }
      break;
    case DECIMAL:
      HiveDecimal decimalValue = ((HiveDecimalObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (decimalValue != null) {
        exec.setVariable(name, new Var(decimalValue.bigDecimalValue()));
      }
      break;
    case VARCHAR:
      HiveVarchar varcharValue = ((HiveVarcharObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (varcharValue != null) {
        exec.setVariable(name, new Var(varcharValue.getValue()));
      }
      break;
    case CHAR:
      HiveChar charValue = ((HiveCharObjectInspector) objectInspector).getPrimitiveJavaObject(inputObject);
      if (charValue != null) {
        exec.setVariable(name, new Var(charValue.getStrippedValue()));
      }
      break;
    default:
      exec.setVariableToNull(name);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "hplsql";
  }
}
