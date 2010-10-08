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
package org.apache.hadoop.hive.serde2.lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Tests LazyArray, LazyMap, LazyStruct and LazyUnion
 *
 */
public class TestLazyArrayMapStruct extends TestCase {

  /**
   * Test the LazyArray class.
   */
  public void testLazyArray() throws Throwable {
    try {
      // Array of Byte
      Text nullSequence = new Text("\\N");
      ObjectInspector oi = LazyFactory.createLazyObjectInspector(TypeInfoUtils
          .getTypeInfosFromTypeString("array<tinyint>").get(0),
          new byte[] {(byte) 1}, 0, nullSequence, false, (byte) 0);
      LazyArray b = (LazyArray) LazyFactory.createLazyObject(oi);
      byte[] data = new byte[] {'-', '1', 1, '\\', 'N', 1, '8'};
      TestLazyPrimitive.initLazyObject(b, data, 0, data.length);

      assertNull(b.getListElementObject(-1));
      assertEquals(new ByteWritable((byte) -1), ((LazyByte) b
          .getListElementObject(0)).getWritableObject());
      assertEquals(new ByteWritable((byte) -1), ((LazyByte) b.getList().get(0))
          .getWritableObject());
      assertNull(b.getListElementObject(1));
      assertNull(b.getList().get(1));
      assertEquals(new ByteWritable((byte) 8), ((LazyByte) b
          .getListElementObject(2)).getWritableObject());
      assertEquals(new ByteWritable((byte) 8), ((LazyByte) b.getList().get(2))
          .getWritableObject());
      assertNull(b.getListElementObject(3));
      assertEquals(3, b.getList().size());

      // Array of String
      oi = LazyFactory.createLazyObjectInspector(TypeInfoUtils
          .getTypeInfosFromTypeString("array<string>").get(0),
          new byte[] {(byte) '\t'}, 0, nullSequence, false, (byte) 0);
      b = (LazyArray) LazyFactory.createLazyObject(oi);
      data = new byte[] {'a', 'b', '\t', 'c', '\t', '\\', 'N', '\t', '\t', 'd'};
      // Note: the first and last element of the byte[] are NOT used
      TestLazyPrimitive.initLazyObject(b, data, 1, data.length - 2);
      assertNull(b.getListElementObject(-1));
      assertEquals(new Text("b"), ((LazyString) b.getListElementObject(0))
          .getWritableObject());
      assertEquals(new Text("b"), ((LazyString) b.getList().get(0))
          .getWritableObject());
      assertEquals(new Text("c"), ((LazyString) b.getListElementObject(1))
          .getWritableObject());
      assertEquals(new Text("c"), ((LazyString) b.getList().get(1))
          .getWritableObject());
      assertNull((b.getListElementObject(2)));
      assertNull((b.getList().get(2)));
      assertEquals(new Text(""), ((LazyString) b.getListElementObject(3))
          .getWritableObject());
      assertEquals(new Text(""), ((LazyString) b.getList().get(3))
          .getWritableObject());
      assertEquals(new Text(""), ((LazyString) b.getListElementObject(4))
          .getWritableObject());
      assertEquals(new Text(""), ((LazyString) b.getList().get(4))
          .getWritableObject());
      assertNull((b.getListElementObject(5)));
      assertEquals(5, b.getList().size());

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Test the LazyMap class.
   */
  public void testLazyMap() throws Throwable {
    try {
      {
        // Map of Integer to String
        Text nullSequence = new Text("\\N");
        ObjectInspector oi = LazyFactory
            .createLazyObjectInspector(TypeInfoUtils
            .getTypeInfosFromTypeString("map<int,string>").get(0),
            new byte[] {(byte) 1, (byte) 2}, 0, nullSequence, false,
            (byte) 0);
        LazyMap b = (LazyMap) LazyFactory.createLazyObject(oi);
        byte[] data = new byte[] {'2', 2, 'd', 'e', 'f', 1, '-', '1', 2, '\\',
            'N', 1, '0', 2, '0', 1, '8', 2, 'a', 'b', 'c'};
        TestLazyPrimitive.initLazyObject(b, data, 0, data.length);

        assertEquals(new Text("def"), ((LazyString) b
            .getMapValueElement(new IntWritable(2))).getWritableObject());
        assertNull(b.getMapValueElement(new IntWritable(-1)));
        assertEquals(new Text("0"), ((LazyString) b
            .getMapValueElement(new IntWritable(0))).getWritableObject());
        assertEquals(new Text("abc"), ((LazyString) b
            .getMapValueElement(new IntWritable(8))).getWritableObject());
        assertNull(b.getMapValueElement(new IntWritable(12345)));

        assertEquals("{2:'def',-1:null,0:'0',8:'abc'}".replace('\'', '\"'),
            SerDeUtils.getJSONString(b, oi));
      }

      {
        // Map of String to String
        Text nullSequence = new Text("\\N");
        ObjectInspector oi = LazyFactory.createLazyObjectInspector(
            TypeInfoUtils.getTypeInfosFromTypeString("map<string,string>").get(
            0), new byte[] {(byte) '#', (byte) '\t'}, 0, nullSequence,
            false, (byte) 0);
        LazyMap b = (LazyMap) LazyFactory.createLazyObject(oi);
        byte[] data = new byte[] {'2', '\t', 'd', '\t', 'f', '#', '2', '\t',
            'd', '#', '-', '1', '#', '0', '\t', '0', '#', '8', '\t', 'a', 'b', 'c'};
        TestLazyPrimitive.initLazyObject(b, data, 0, data.length);

        assertEquals(new Text("d\tf"), ((LazyString) b
            .getMapValueElement(new Text("2"))).getWritableObject());
        assertNull(b.getMapValueElement(new Text("-1")));
        assertEquals(new Text("0"), ((LazyString) b
            .getMapValueElement(new Text("0"))).getWritableObject());
        assertEquals(new Text("abc"), ((LazyString) b
            .getMapValueElement(new Text("8"))).getWritableObject());
        assertNull(b.getMapValueElement(new Text("-")));

        assertEquals("{'2':'d\\tf','2':'d','-1':null,'0':'0','8':'abc'}"
            .replace('\'', '\"'), SerDeUtils.getJSONString(b, oi));
      }

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Test the LazyStruct class.
   */
  public void testLazyStruct() throws Throwable {
    try {
      {
        ArrayList<TypeInfo> fieldTypeInfos = TypeInfoUtils
            .getTypeInfosFromTypeString("int,array<string>,map<string,string>,string");
        List<String> fieldNames = Arrays.asList(new String[] {"a", "b", "c",
            "d"});
        Text nullSequence = new Text("\\N");

        ObjectInspector oi = LazyFactory.createLazyStructInspector(fieldNames,
            fieldTypeInfos, new byte[] {' ', ':', '='}, nullSequence, false,
            false, (byte) 0);
        LazyStruct o = (LazyStruct) LazyFactory.createLazyObject(oi);

        Text data;

        data = new Text("123 a:b:c d=e:f=g hi");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data
            .getLength());
        assertEquals(
            "{'a':123,'b':['a','b','c'],'c':{'d':'e','f':'g'},'d':'hi'}"
            .replace("'", "\""), SerDeUtils.getJSONString(o, oi));

        data = new Text("123 \\N d=e:f=g \\N");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data
            .getLength());
        assertEquals("{'a':123,'b':null,'c':{'d':'e','f':'g'},'d':null}"
            .replace("'", "\""), SerDeUtils.getJSONString(o, oi));

        data = new Text("\\N a d=\\N:f=g:h no tail");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data
            .getLength());
        assertEquals(
            "{'a':null,'b':['a'],'c':{'d':null,'f':'g','h':null},'d':'no'}"
            .replace("'", "\""), SerDeUtils.getJSONString(o, oi));

        data = new Text("\\N :a:: \\N no tail");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data
            .getLength());
        assertEquals("{'a':null,'b':['','a','',''],'c':null,'d':'no'}".replace(
            "'", "\""), SerDeUtils.getJSONString(o, oi));

        data = new Text("123   ");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data
            .getLength());
        assertEquals("{'a':123,'b':[],'c':{},'d':''}".replace("'", "\""),
            SerDeUtils.getJSONString(o, oi));

        data = new Text(": : : :");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data
            .getLength());
        assertEquals("{'a':null,'b':['',''],'c':{'':null,'':null},'d':':'}"
            .replace("'", "\""), SerDeUtils.getJSONString(o, oi));

        data = new Text("= = = =");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data
            .getLength());
        assertEquals("{'a':null,'b':['='],'c':{'':''},'d':'='}".replace("'",
            "\""), SerDeUtils.getJSONString(o, oi));

        // test LastColumnTakesRest
        oi = LazyFactory.createLazyStructInspector(Arrays.asList(new String[] {
            "a", "b", "c", "d"}), fieldTypeInfos,
            new byte[] {' ', ':', '='}, nullSequence, true, false, (byte) 0);
        o = (LazyStruct) LazyFactory.createLazyObject(oi);
        data = new Text("\\N a d=\\N:f=g:h has tail");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data
            .getLength());
        assertEquals(
            "{'a':null,'b':['a'],'c':{'d':null,'f':'g','h':null},'d':'has tail'}"
            .replace("'", "\""), SerDeUtils.getJSONString(o, oi));
      }
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Test the LazyUnion class.
   */
  public void testLazyUnion() throws Throwable {
    try {
      {
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
            "uniontype<int,array<string>,map<string,string>,string>");
        Text nullSequence = new Text("\\N");

        ObjectInspector oi = LazyFactory.createLazyObjectInspector(typeInfo,
            new byte[] {'^', ':', '='}, 0, nullSequence, false, (byte) 0);
        LazyUnion o = (LazyUnion) LazyFactory.createLazyObject(oi);

        Text data;

        data = new Text("0^123");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0,
            data.getLength());
        assertEquals("{0:123}", SerDeUtils.getJSONString(o, oi));

        data = new Text("1^a:b:c");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0,
            data.getLength());
        assertEquals(
            "{1:[\"a\",\"b\",\"c\"]}", SerDeUtils.getJSONString(o, oi));

        data = new Text("2^d=e:f=g");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0,
            data.getLength());
        assertEquals(
            "{2:{\"d\":\"e\",\"f\":\"g\"}}", SerDeUtils.getJSONString(o, oi));

        data = new Text("3^hi");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0,
            data.getLength());
        assertEquals("{3:\"hi\"}", SerDeUtils.getJSONString(o, oi));


        data = new Text("0^\\N");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0,
            data.getLength());
        assertEquals("{0:null}", SerDeUtils.getJSONString(o, oi));

        data = new Text("1^ :a::");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0,
            data.getLength());
        assertEquals(
            "{1:[\" \",\"a\",\"\",\"\"]}", SerDeUtils.getJSONString(o, oi));

        data = new Text("2^d=\\N:f=g:h");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0,
            data.getLength());
        assertEquals(
            "{2:{\"d\":null,\"f\":\"g\",\"h\":null}}",
            SerDeUtils.getJSONString(o, oi));

        data = new Text("2^= ");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0,
            data.getLength());
        assertEquals("{2:{\"\":\" \"}}", SerDeUtils.getJSONString(o, oi));
      }
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}
