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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.udf.UDFType;

/**
 * A User-defined function (UDF) for use with Hive.
 * <p>
 * New UDF classes need to inherit from this UDF class (or from {@link
 * org.apache.hadoop.hive.ql.udf.generic.GenericUDF GenericUDF} which provides more flexibility at
 * the cost of more complexity).
 * <p>
 * Requirements for all classes extending this UDF are:
 * <ul>
 * <li>Implement one or more methods named {@code evaluate} which will be called by Hive (the exact
 * way in which Hive resolves the method to call can be configured by setting a custom {@link
 * UDFMethodResolver}). The following are some examples:
 * <ul>
 * <li>{@code public int evaluate();}</li>
 * <li>{@code public int evaluate(int a);}</li>
 * <li>{@code public double evaluate(int a, double b);}</li>
 * <li>{@code public String evaluate(String a, int b, Text c);}</li>
 * <li>{@code public Text evaluate(String a);}</li>
 * <li>{@code public String evaluate(List<Integer> a);} (Note that Hive Arrays are represented as
 * {@link java.util.List Lists} in Hive.
 * So an {@code ARRAY<int>} column would be passed in as a {@code List<Integer>}.)</li>
 * </ul>
 * </li>
 * <li>{@code evaluate} should never be a void method. However it can return {@code null} if
 * needed.
 * <li>Return types as well as method arguments can be either Java primitives or the corresponding
 * {@link org.apache.hadoop.io.Writable Writable} class.</li>
 * </ul>
 * One instance of this class will be instantiated per JVM and it will not be called concurrently.
 *
 * @see Description
 * @see UDFType
 */
@UDFType(deterministic = true)
public class UDF {

  /**
   * The resolver to use for method resolution.
   */
  private UDFMethodResolver rslv;

  /**
   * The constructor.
   */
  public UDF() {
    rslv = new DefaultUDFMethodResolver(this.getClass());
  }

  /**
   * The constructor with user-provided {@link UDFMethodResolver}.
   */
  protected UDF(UDFMethodResolver rslv) {
    this.rslv = rslv;
  }

  /**
   * Sets the resolver.
   *
   * @param rslv The method resolver to use for method resolution.
   */
  public void setResolver(UDFMethodResolver rslv) {
    this.rslv = rslv;
  }

  /**
   * Get the method resolver.
   */
  public UDFMethodResolver getResolver() {
    return rslv;
  }

  /**
   * This can be overridden to include JARs required by this UDF.
   *
   * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF#getRequiredJars()
   *      GenericUDF.getRequiredJars()
   *
   * @return an array of paths to files to include, {@code null} by default.
   */
  public String[] getRequiredJars() {
    return null;
  }

  /**
   * This can be overridden to include files required by this UDF.
   *
   * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF#getRequiredFiles()
   *      GenericUDF.getRequiredFiles()
   *
   * @return an array of paths to files to include, {@code null} by default.
   */
  public String[] getRequiredFiles() {
    return null;
  }
}
