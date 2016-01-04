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
package hive.it.custom.udfs;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public class UDF2 extends GenericUDF {
  public UDF2() {
    Util util = new Util();
    System.out.println(
        "constructor: " + getClass().getSimpleName() + " classloader: " + getClass().getClassLoader() + ", " + util
            + " classloader: "
            + util.getClass().getClassLoader());
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    Util util = new Util();
    System.out.println(
        "initialize: " + getClass().getSimpleName() + " classloader: " + getClass().getClassLoader() + ", " + util
            + " classloader: "
            + util.getClass().getClassLoader());
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Util util = new Util();
    System.out.println(
        "evaluate: " + getClass().getSimpleName() + " classloader: " + getClass().getClassLoader() + ", " + util
            + " classloader: "
            + util.getClass().getClassLoader());
    return getClass().getSimpleName();
  }

  @Override
  public String getDisplayString(String[] children) {
    return getClass().getName();
  }
}
