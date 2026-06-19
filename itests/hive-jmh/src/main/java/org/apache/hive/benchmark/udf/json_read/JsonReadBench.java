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

package org.apache.hive.benchmark.udf.json_read;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFJsonRead;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

public class JsonReadBench {

  @State(Scope.Thread)
  public static class MyState {

    public final String json;
    public final String type;

    public MyState() {
      try {
        json = getResource("val1.json");
        type = getResource("val1.type").toLowerCase().trim();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private String getResource(String fname) throws IOException {
      return IOUtils.toString(JsonReadBench.class.getResourceAsStream(fname), Charset.defaultCharset());
    }
  }

  public void checkBenchMarkMethod() throws Exception {
    benchmarkMethod(new MyState());
  }

  @Benchmark
  public void benchmarkMethod(MyState state) throws Exception {
    try (GenericUDFJsonRead udf = new GenericUDFJsonRead()) {
      ObjectInspector[] arguments = buildArguments(state.type);
      udf.initialize(arguments);

      udf.evaluate(evalArgs(state.json));
    }
  }

  private DeferredObject[] evalArgs(String string) {
    return new DeferredObject[] { new GenericUDF.DeferredJavaObject(new Text(string)), null };
  }

  private ObjectInspector[] buildArguments(String typeStr) {
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI, PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text(typeStr)) };
    return arguments;
  }

}
