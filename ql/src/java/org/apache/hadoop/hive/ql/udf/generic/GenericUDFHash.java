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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * GenericUDF Class for computing hash values.
 */
@description(name = "hash", value = "_FUNC_(a1, a2, ...) - Returns a hash value of the arguments")
public class GenericUDFHash extends GenericUDF {

  private static Log LOG = LogFactory.getLog(GenericUDFHash.class.getName());

  ObjectInspector[] argumentOIs;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentTypeException {

    argumentOIs = arguments;
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  IntWritable result = new IntWritable();

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    // See
    // http://java.sun.com/j2se/1.5.0/docs/api/java/util/List.html#hashCode()
    int r = 0;
    for (int i = 0; i < arguments.length; i++) {
      r = r * 31
          + ObjectInspectorUtils.hashCode(arguments[i].get(), argumentOIs[i]);
    }
    result.set(r);
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "hash(" + StringUtils.join(children, ',') + ")";
  }

}
