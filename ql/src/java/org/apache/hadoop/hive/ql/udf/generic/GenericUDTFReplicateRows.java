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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

/**
 * Takes a row of data and repeats n times.
 */
@Description(name = "replicate_rows", value = "_FUNC_(n, cols...) - turns 1 row into n rows")
public class GenericUDTFReplicateRows extends GenericUDTF {
  @Override
  public void close() throws HiveException {
  }

  private transient List<ObjectInspector> argOIs = new ArrayList<ObjectInspector>();

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length < 2) {
      throw new UDFArgumentException("UDTFReplicateRows() expects at least two arguments.");
    }
    if (!(args[0] instanceof LongObjectInspector)) {
      throw new UDFArgumentException(
          "The first argument to UDTFReplicateRows() must be a long (got "
              + args[0].getTypeName() + " instead).");
    }

    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    for (int index = 0; index < args.length; ++index) {
      fieldNames.add("col" + index);
      fieldOIs.add(args[index]);
    }
    argOIs = fieldOIs;
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException, UDFArgumentException {

    long numRows = ((LongObjectInspector) argOIs.get(0)).get(args[0]);

    for (long n = 0; n < numRows; n++) {
      forward(args);
    }
  }

  @Override
  public String toString() {
    return "UDTFReplicateRows";
  }

}
