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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * A Generic User-defined function (GenericUDF) for the use with Hive.
 * 
 * New GenericUDF classes need to inherit from this GenericUDF class.
 * 
 * The GenericUDF are superior to normal UDFs in the following ways:
 * 1. It can accept arguments of complex types, and return complex types.
 * 2. It can accept variable length of arguments.
 * 3. It can accept an infinite number of function signature - for example, 
 *    it's easy to write a GenericUDF that accepts array<int>, 
 *    array<array<int>> and so on (arbitrary levels of nesting).
 * 4. It can do short-circuit evaluations using DeferedObject.  
 */
@UDFType(deterministic=true)
public abstract class GenericUDF {
  
  /**
   * A Defered Object allows us to do lazy-evaluation
   * and short-circuiting.
   * GenericUDF use DeferedObject to pass arguments.
   */
  public static interface DeferredObject {
    public Object get() throws HiveException; 
  };
  
  /**
   * The constructor
   */
  public GenericUDF() {
  }

  /**
   * Initialize this GenericUDF. This will be called once and only once per
   * GenericUDF instance.
   * 
   * @param arguments     The ObjectInspector for the arguments
   * @throws UDFArgumentException
   *                      Thrown when arguments have wrong types, wrong length, etc.
   * @return              The ObjectInspector for the return value
   */
  public abstract ObjectInspector initialize(ObjectInspector[] arguments) 
      throws UDFArgumentException;
  
  /**
   * Evaluate the GenericUDF with the arguments.
   * @param arguments  The arguments as DeferedObject, use DeferedObject.get() to
   *                   get the actual argument Object.  The Objects can be inspected
   *                   by the ObjectInspectors passed in the initialize call.
   * @return The 
   */
  public abstract Object evaluate(DeferredObject[] arguments) throws HiveException;
  
  /**
   * Get the String to be displayed in explain.
   */
  public abstract String getDisplayString(String[] children);
  
}
