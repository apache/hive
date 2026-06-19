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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;

public abstract class AbstractTestGenericUDFOPNumeric {
  public AbstractTestGenericUDFOPNumeric() {
    // Arithmetic operations rely on getting conf from SessionState, need to initialize here.
    SessionState ss = new SessionState(new HiveConf());
    ss.getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");
    SessionState.setCurrentSessionState(ss);
  }

  protected void verifyReturnType(GenericUDF udf,
      String typeStr1, String typeStr2, String expectedTypeStr) throws HiveException {
    // Lookup type infos for our input types and expected return type
    PrimitiveTypeInfo type1 = TypeInfoFactory.getPrimitiveTypeInfo(typeStr1);
    PrimitiveTypeInfo type2 = TypeInfoFactory.getPrimitiveTypeInfo(typeStr2);
    PrimitiveTypeInfo expectedType = TypeInfoFactory.getPrimitiveTypeInfo(expectedTypeStr);

    // Initialize UDF which will output the return type for the UDF.
    ObjectInspector[] inputOIs = {
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type1),
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type2)
    };
    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);

    Assert.assertEquals("Return type for " + udf.getDisplayString(new String[] {typeStr1, typeStr2}),
        expectedType, oi.getTypeInfo());
  }
}
