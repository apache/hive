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
 
package hive.it.custom.udfs; 

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import hive.it.custom.udfs.vector.VectorStringRot13;

@VectorizedExpressions(value = { VectorStringRot13.class })
public class GenericUDFRot13 extends GenericUDF {

  @Override
  public Object evaluate(DeferredObject[] arg0) throws HiveException {
    /* this is the bad part - the vectorized UDF returns the right result */
    return new Text("Unvectorized");
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return String.format("Rot13(%s)", arg0[0]);
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arg0)
      throws UDFArgumentException {
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

}
