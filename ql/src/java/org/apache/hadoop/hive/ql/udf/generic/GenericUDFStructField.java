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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFStructField;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * The class shouldn't be used, and only to align the implementation of vectorization UDF for
 * struct field.
 */
@Description(
    name = "structField",
    value = "The class shouldn't be used, and only to align the implementation" +
        " of vectorization UDF for struct field")
@VectorizedExpressions({VectorUDFStructField.class})
public class GenericUDFStructField extends GenericUDF {

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    throw new UDFArgumentException("GenericUDFStructField shouldn't be used directly.");
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    throw new HiveException("GenericUDFStructField shouldn't be used directly.");
  }

  @Override
  public String getDisplayString(String[] children) {
    throw new RuntimeException("GenericUDFStructField shouldn't be used directly.");
  }
}
