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
package org.apache.hadoop.hive.ql.exec;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.KeyWrapperFactory.ListKeyWrapper;
import org.apache.hadoop.hive.ql.exec.KeyWrapperFactory.TextKeyWrapper;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class TestKeyWrapperFactory {
  private KeyWrapperFactory factory;


  @Before
  public void setup() throws Exception {
    SessionState ss = new SessionState(new HiveConf());
    SessionState.setCurrentSessionState(ss);

    ArrayList<Text> col1 = new ArrayList<Text>();
    col1.add(new Text("0"));
    col1.add(new Text("1"));
    col1.add(new Text("2"));
    col1.add(new Text("3"));
    TypeInfo col1Type = TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo);
    ArrayList<Text> cola = new ArrayList<Text>();
    cola.add(new Text("a"));
    cola.add(new Text("b"));
    cola.add(new Text("c"));
    TypeInfo colaType = TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo);
    try {
      ArrayList<Object> data = new ArrayList<Object>();
      data.add(col1);
      data.add(cola);
      ArrayList<String> names = new ArrayList<String>();
      names.add("col1");
      names.add("cola");
      ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
      typeInfos.add(col1Type);
      typeInfos.add(colaType);
      TypeInfo dataType = TypeInfoFactory.getStructTypeInfo(names, typeInfos);

      InspectableObject r = new InspectableObject();
      ObjectInspector[] oi = new ObjectInspector[1];
      r.o = data;
      oi[0]= TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(dataType);
      try {
        // get a evaluator for a simple field expression
        ExprNodeDesc exprDesc = new ExprNodeColumnDesc(colaType, "cola", "",
            false);
        ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(exprDesc);
        ExprNodeEvaluator[] evals = new ExprNodeEvaluator[1];
        evals[0] = eval;
        ObjectInspector resultOI = eval.initialize(oi[0]);
        ObjectInspector[] resultOIs = new ObjectInspector[1];
        resultOIs[0] = resultOI;
        factory = new KeyWrapperFactory(evals, oi, resultOIs);
      } catch (Throwable e) {
        e.printStackTrace();
        throw e;
      }
    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testKeyWrapperEqualsCopy() throws Exception {
    KeyWrapper w1 = factory.getKeyWrapper();
    KeyWrapper w2 = w1.copyKey();
    assertTrue(w1.equals(w2));
  }

  @Test
  public void testDifferentWrapperTypesUnequal() {
    TextKeyWrapper w3 = factory.new TextKeyWrapper(false);
    ListKeyWrapper w4 = factory.new ListKeyWrapper(false);
    assertFalse(w3.equals(w4));
    assertFalse(w4.equals(w3));
  }
}
