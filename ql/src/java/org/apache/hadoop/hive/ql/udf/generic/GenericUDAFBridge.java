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
package org.apache.hadoop.hive.ql.udf.generic;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class is a bridge between GenericUDAF and UDAF. Old UDAF can be used
 * with the GenericUDAF infrastructure through this bridge.
 */
public class GenericUDAFBridge implements GenericUDAFResolver {

  UDAF udaf;

  public GenericUDAFBridge(UDAF udaf) {
    this.udaf = udaf;
  }

  public Class<? extends UDAF> getUDAFClass() {
    return udaf.getClass();
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {

    Class<? extends UDAFEvaluator> udafEvaluatorClass = udaf.getResolver()
        .getEvaluatorClass(Arrays.asList(parameters));

    return new GenericUDAFBridgeEvaluator(udafEvaluatorClass);
  }

  public static class GenericUDAFBridgeEvaluator extends GenericUDAFEvaluator
      implements Serializable {

    private static final long serialVersionUID = 1L;

    // Used by serialization only
    public GenericUDAFBridgeEvaluator() {
    }

    public Class<? extends UDAFEvaluator> getUdafEvaluator() {
      return udafEvaluator;
    }

    public void setUdafEvaluator(Class<? extends UDAFEvaluator> udafEvaluator) {
      this.udafEvaluator = udafEvaluator;
    }

    public GenericUDAFBridgeEvaluator(
        Class<? extends UDAFEvaluator> udafEvaluator) {
      this.udafEvaluator = udafEvaluator;
    }

    Class<? extends UDAFEvaluator> udafEvaluator;

    transient ObjectInspector[] parameterOIs;
    transient Object result;

    transient Method iterateMethod;
    transient Method mergeMethod;
    transient Method terminatePartialMethod;
    transient Method terminateMethod;

    transient ConversionHelper conversionHelper;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      parameterOIs = parameters;

      // Get the reflection methods from ue
      for (Method method : udafEvaluator.getMethods()) {
        if (method.getName().equals("iterate")) {
          iterateMethod = method;
        }
        if (method.getName().equals("merge")) {
          mergeMethod = method;
        }
        if (method.getName().equals("terminatePartial")) {
          terminatePartialMethod = method;
        }
        if (method.getName().equals("terminate")) {
          terminateMethod = method;
        }
      }

      // Input: do Java/Writable conversion if needed
      Method aggregateMethod = null;
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        aggregateMethod = iterateMethod;
      } else {
        aggregateMethod = mergeMethod;
      }
      conversionHelper = new ConversionHelper(aggregateMethod, parameters);

      // Output: get the evaluate method
      Method evaluateMethod = null;
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        evaluateMethod = terminatePartialMethod;
      } else {
        evaluateMethod = terminateMethod;
      }
      // Get the output ObjectInspector from the return type.
      Type returnType = evaluateMethod.getGenericReturnType();
      try {
        return ObjectInspectorFactory.getReflectionObjectInspector(returnType,
            ObjectInspectorOptions.JAVA);
      } catch (RuntimeException e) {
        throw new HiveException("Cannot recognize return type " + returnType
            + " from " + evaluateMethod, e);
      }
    }

    /** class for storing UDAFEvaluator value */
    static class UDAFAgg implements AggregationBuffer {
      UDAFEvaluator ueObject;

      UDAFAgg(UDAFEvaluator ueObject) {
        this.ueObject = ueObject;
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() {
      return new UDAFAgg((UDAFEvaluator) ReflectionUtils.newInstance(
          udafEvaluator, null));
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((UDAFAgg) agg).ueObject.init();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      FunctionRegistry.invoke(iterateMethod, ((UDAFAgg) agg).ueObject,
          conversionHelper.convertIfNecessary(parameters));
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      FunctionRegistry.invoke(mergeMethod, ((UDAFAgg) agg).ueObject,
          conversionHelper.convertIfNecessary(partial));
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return FunctionRegistry.invoke(terminateMethod, ((UDAFAgg) agg).ueObject);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return FunctionRegistry.invoke(terminatePartialMethod,
          ((UDAFAgg) agg).ueObject);
    }

  }

}
