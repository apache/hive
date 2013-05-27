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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

/**
 * A Generic User-defined function (GenericUDF) for the use with Hive.
 *
 * New GenericUDF classes need to inherit from this GenericUDF class.
 *
 * The GenericUDF are superior to normal UDFs in the following ways: 1. It can
 * accept arguments of complex types, and return complex types. 2. It can accept
 * variable length of arguments. 3. It can accept an infinite number of function
 * signature - for example, it's easy to write a GenericUDF that accepts
 * array<int>, array<array<int>> and so on (arbitrary levels of nesting). 4. It
 * can do short-circuit evaluations using DeferedObject.
 */
@UDFType(deterministic = true)
public abstract class GenericUDF implements Closeable {

  /**
   * A Defered Object allows us to do lazy-evaluation and short-circuiting.
   * GenericUDF use DeferedObject to pass arguments.
   */
  public static interface DeferredObject {
    void prepare(int version) throws HiveException;
    Object get() throws HiveException;
  };

  /**
   * A basic dummy implementation of DeferredObject which just stores a Java
   * Object reference.
   */
  public static class DeferredJavaObject implements DeferredObject {
    private Object value;

    public DeferredJavaObject(Object value) {
      this.value = value;
    }

    @Override
    public void prepare(int version) throws HiveException {
    }

    @Override
    public Object get() throws HiveException {
      return value;
    }
  }

  /**
   * The constructor.
   */
  public GenericUDF() {
  }

  /**
   * Initialize this GenericUDF. This will be called once and only once per
   * GenericUDF instance.
   *
   * @param arguments
   *          The ObjectInspector for the arguments
   * @throws UDFArgumentException
   *           Thrown when arguments have wrong types, wrong length, etc.
   * @return The ObjectInspector for the return value
   */
  public abstract ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException;

  /**
   * Additionally setup GenericUDF with MapredContext before initializing.
   * This is only called in runtime of MapRedTask.
   *
   * @param context context
   */
  public void configure(MapredContext context) {
  }

  /**
   * Initialize this GenericUDF.  Additionally, if the arguments are constant
   * and the function is eligible to be folded, then the constant value
   * returned by this UDF will be computed and stored in the
   * ConstantObjectInspector returned.  Otherwise, the function behaves exactly
   * like initialize().
   */
  public ObjectInspector initializeAndFoldConstants(ObjectInspector[] arguments)
      throws UDFArgumentException {

    ObjectInspector oi = initialize(arguments);

    // If the UDF depends on any external resources, we can't fold because the
    // resources may not be available at compile time.
    if (getRequiredFiles() != null ||
        getRequiredJars() != null) {
      return oi;
    }

    boolean allConstant = true;
    for (int ii = 0; ii < arguments.length; ++ii) {
      if (!ObjectInspectorUtils.isConstantObjectInspector(arguments[ii])) {
        allConstant = false;
        break;
      }
    }

    if (allConstant &&
        !ObjectInspectorUtils.isConstantObjectInspector(oi) &&
        FunctionRegistry.isDeterministic(this) &&
        !FunctionRegistry.isStateful(this) &&
        ObjectInspectorUtils.supportsConstantObjectInspector(oi)) {
      DeferredObject[] argumentValues =
        new DeferredJavaObject[arguments.length];
      for (int ii = 0; ii < arguments.length; ++ii) {
        argumentValues[ii] = new DeferredJavaObject(
            ((ConstantObjectInspector)arguments[ii]).getWritableConstantValue());
      }
      try {
        Object constantValue = evaluate(argumentValues);
        oi = ObjectInspectorUtils.getConstantObjectInspector(oi, constantValue);
      } catch (HiveException e) {
        throw new UDFArgumentException(e);
      }
    }
    return oi;
  }

  /**
   * The following two functions can be overridden to automatically include
   * additional resources required by this UDF.  The return types should be
   * arrays of paths.
   */
  public String[] getRequiredJars() {
    return null;
  }

  public String[] getRequiredFiles() {
    return null;
  }

  /**
   * Evaluate the GenericUDF with the arguments.
   *
   * @param arguments
   *          The arguments as DeferedObject, use DeferedObject.get() to get the
   *          actual argument Object. The Objects can be inspected by the
   *          ObjectInspectors passed in the initialize call.
   * @return The
   */
  public abstract Object evaluate(DeferredObject[] arguments)
      throws HiveException;

  /**
   * Get the String to be displayed in explain.
   */
  public abstract String getDisplayString(String[] children);

  /**
   * Close GenericUDF.
   * This is only called in runtime of MapRedTask.
   */
  public void close() throws IOException {
  }
}
