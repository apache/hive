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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

@UDFType(deterministic = false)
@Description(name = "assert_true_oom",
        value = "_FUNC_(condition) - " +
                "Throw an MapJoinMemoryExhaustionError if 'condition' is not true.")
public class GenericUDFAssertTrueOOM extends GenericUDF {
  private ObjectInspectorConverters.Converter conditionConverter = null;

  public GenericUDFAssertTrueOOM() {
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    HiveConf conf = SessionState.getSessionConf();
    if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
      throw new RuntimeException("this UDF is only available in testmode");
    }
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException("ASSERT_TRUE_OOM() expects one argument.");
    }
    if (arguments[0].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Argument to ASSERT_TRUE_OOM() should be primitive.");
    }
    conditionConverter =
        ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);

    return PrimitiveObjectInspectorFactory.writableVoidObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    BooleanWritable condition = (BooleanWritable) conditionConverter.convert(arguments[0].get());
    if (condition == null || !condition.get()) {
      throw new MapJoinMemoryExhaustionError("assert_true_oom: assertion failed; Simulated OOM");
    }
    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("assert_true_oom", children);
  }
}
