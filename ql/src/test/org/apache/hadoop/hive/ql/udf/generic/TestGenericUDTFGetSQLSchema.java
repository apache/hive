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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * TestGenericUDTFGetSQLSchema.
 */
public class TestGenericUDTFGetSQLSchema {

  private static SessionState sessionState;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HiveConf conf = new HiveConf();
    conf.set("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider");
    sessionState = SessionState.start(conf);
    sessionState.initTxnMgr(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    SessionState.endStart(sessionState);
  }

  @Test
  public void testWithComplexTypes() throws Exception {
    invokeUDTFAndTest("select array('val1','val2') c1," +
            " named_struct('a',1,'b','2') c2, " +
            " array(array(1)) c3," +
            " array(named_struct('a',1,'b','2')) c4," +
            " map(1,1) c5",
        new String[]{"c1", "array<string>",
            "c2", "struct<a:int,b:string>",
            "c3", "array<array<int>>",
            "c4", "array<struct<a:int,b:string>>",
            "c5", "map<int,int>"
        });
  }

  @Test
  public void testWithSimpleTypes() throws Exception {
    invokeUDTFAndTest("select 1 as c1, 'Happy Valentines Day' as c2, 2.2 as c3, cast(2.2 as float) c4, " +
            "cast(2.2 as double) c5, " +
            "cast('2019-02-14' as date) c6",
        new String[]{"c1", "int",
            "c2", "string",
            "c3", "decimal(2,1)",
            "c4", "float",
            "c5", "double",
            "c6", "date"
        });
  }

  @Test
  public void testWithDDL() throws Exception {
    invokeUDTFAndTest("show tables", new String[]{});
  }

  private void invokeUDTFAndTest(String query, String[] expected) throws HiveException {

    GenericUDTFGetSQLSchema genericUDTFGetSQLSchema = new GenericUDTFGetSQLSchema();
    List<String> actual = new ArrayList<>();
    genericUDTFGetSQLSchema.collector = input -> {
      if (input != null) {
        Object[] udfOutput = (Object[]) input;
        actual.add(new String((byte[]) udfOutput[0]));
        actual.add(new String((byte[]) udfOutput[1]));
      }
    };

    genericUDTFGetSQLSchema
        .initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaStringObjectInspector});
    genericUDTFGetSQLSchema.process(new Object[]{query});

    assertEquals(expected.length, actual.size());
    assertTrue("Failed for query: " + query + ". Expected: " + Arrays.toString(expected)
        + ". Actual: " + actual, Arrays.equals(expected, actual.toArray()));
  }

}
