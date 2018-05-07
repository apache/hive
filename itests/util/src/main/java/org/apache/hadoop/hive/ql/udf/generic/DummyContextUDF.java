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
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;
@Description(name = "counter",
value = "_FUNC_(col) - UDF to report MR counter values")
public class DummyContextUDF extends GenericUDF {

  private MapredContext context;
  private LongWritable result = new LongWritable();

  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
  }

  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Reporter reporter = context.getReporter();
    Counters.Counter counter = reporter.getCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS");
    result.set(counter.getValue());
    return result;
  }

  public String getDisplayString(String[] children) {
    return "dummy-func()";
  }

  @Override
    public void configure(MapredContext context) {
    this.context = context;
  }
}
