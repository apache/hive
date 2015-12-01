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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

// deterministic in the query range
@Description(name = "current_database",
    value = "_FUNC_() - returns currently using database name")
@NDV(maxNdv = 1)
public class UDFCurrentDB extends GenericUDF {

  private MapredContext context;

  @Override
  public void configure(MapredContext context) {
    this.context = context;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    String database;
    if (context != null) {
      database = context.getJobConf().get("hive.current.database");
    } else {
      database = SessionState.get().getCurrentDatabase();
    }
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text(database));
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return SessionState.get().getCurrentDatabase();
  }

  @Override
  public String getDisplayString(String[] children) {
    return "current_database()";
  }
}
