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
package org.apache.hadoop.hive.serde2.lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryFactory;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Tests LazyArray, LazyMap, LazyStruct and LazyUnion
 *
 */
public class TestLazyArrayMapStruct extends TestCase {

  // nesting level limits
  static final int EXTENDED_LEVEL_THRESHOLD = 24;
  static final int DEFAULT_LEVEL_THRESHOLD = 8;

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

      // -- HIVE-4149
      b = (LazyArray) LazyFactory.createLazyObject(oi);

      data = new byte[] {'a', '\t', '\\', 'N'};
      TestLazyPrimitive.initLazyObject(b, data, 0, data.length);
      assertEquals(new Text("a"), ((LazyString) b.getListElementObject(0)).getWritableObject());
      assertNull(b.getListElementObject(1));

      data = new byte[] {'\\', 'N', '\t', 'a'};
      TestLazyPrimitive.initLazyObject(b, data, 0, data.length);
      assertNull(b.getListElementObject(0));
      assertNull(b.getListElementObject(0));  // twice (returns not cleaned cache)
      assertEquals(new Text("a"), ((LazyString) b.getListElementObject(1)).getWritableObject());

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
        assertEquals(4, b.getMapSize());
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

        assertEquals("{'2':'d\\tf','-1':null,'0':'0','8':'abc'}"
            .replace('\'', '\"'), SerDeUtils.getJSONString(b, oi));
        assertEquals(4, b.getMapSize());
      }

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /*
   * test LazyMap with bad entries, e.g., empty key or empty entries
   * where '[' and  ']' don't exist, only for notation purpose,
   * STX with value of 2 as entry separator, ETX with 3 as key/value separator
   * */
  public void testLazyMapWithBadEntries() throws Throwable {
    try {
      {
        // Map of String to String
        Text nullSequence = new Text("");
        ObjectInspector oi = LazyFactory.createLazyObjectInspector(
            TypeInfoUtils.getTypeInfosFromTypeString("map<string,string>").get(
            0), new byte[] {'\2', '\3'}, 0, nullSequence,
            false, (byte) 0);
        LazyMap b = (LazyMap) LazyFactory.createLazyObject(oi);

       //read friendly string: ak[EXT]av[STX]bk[ETX]bv[STX]ck[ETX]cv[STX]dk[ETX]dv
       byte[] data = new byte[] {
            'a', 'k', '\3', 'a', 'v',
            '\02', 'b', 'k', '\3', 'b', 'v',
            '\02', 'c', 'k', '\3', 'c', 'v',
            '\02', 'd', 'k', '\3', 'd', 'v'};
        TestLazyPrimitive.initLazyObject(b, data, 0, data.length);

        assertEquals(new Text("av"), ((LazyString) b
            .getMapValueElement(new Text("ak"))).getWritableObject());
        assertNull(b.getMapValueElement(new Text("-1")));
        assertEquals(new Text("bv"), ((LazyString) b
            .getMapValueElement(new Text("bk"))).getWritableObject());
        assertEquals(new Text("cv"), ((LazyString) b
            .getMapValueElement(new Text("ck"))).getWritableObject());
        assertNull(b.getMapValueElement(new Text("-")));
        assertEquals(new Text("dv"), ((LazyString) b
            .getMapValueElement(new Text("dk"))).getWritableObject());
        assertEquals(4, b.getMapSize());
      }

      {
        // Map of String to String, LazyMap allows empty-string style key, e.g., {"" : null}
        // or {"", ""}, but not null style key, e.g., {null:""}
        Text nullSequence = new Text("");
        ObjectInspector oi = LazyFactory.createLazyObjectInspector(
            TypeInfoUtils.getTypeInfosFromTypeString("map<string,string>").get(
            0), new byte[] {'\2', '\3'}, 0, nullSequence,
            false, (byte) 0);
        LazyMap b = (LazyMap) LazyFactory.createLazyObject(oi);

       //read friendly string: [STX]ak[EXT]av[STX]bk[ETX]bv[STX]ck[ETX]cv[STX]dk[ETX]dv
        byte[] data = new byte[] {
            '\02', 'a', 'k', '\3', 'a', 'v',
            '\02', 'b', 'k', '\3', 'b', 'v',
            '\02', 'c', 'k', '\3', 'c', 'v',
            '\02', 'd', 'k', '\3', 'd', 'v'};
        TestLazyPrimitive.initLazyObject(b, data, 0, data.length);

        assertNull(b.getMapValueElement(new Text(""))); //{"" : null}
        assertEquals(new Text("av"), ((LazyString) b
            .getMapValueElement(new Text("ak"))).getWritableObject());
        assertNull(b.getMapValueElement(new Text("-1")));
        assertEquals(new Text("bv"), ((LazyString) b
            .getMapValueElement(new Text("bk"))).getWritableObject());
        assertEquals(new Text("cv"), ((LazyString) b
            .getMapValueElement(new Text("ck"))).getWritableObject());
        assertNull(b.getMapValueElement(new Text("-")));
        assertEquals(new Text("dv"), ((LazyString) b
            .getMapValueElement(new Text("dk"))).getWritableObject());
        assertEquals(4, b.getMapSize());
      }

      {
        // Map of String to String, LazyMap allows empty-string style key, e.g., {"" : null}
        // or {"", ""}, but not null style key, e.g., {null:""}
        Text nullSequence = new Text("");
        ObjectInspector oi = LazyFactory.createLazyObjectInspector(
            TypeInfoUtils.getTypeInfosFromTypeString("map<string,string>").get(
            0), new byte[] {'\2', '\3'}, 0, nullSequence,
            false, (byte) 0);
        LazyMap b = (LazyMap) LazyFactory.createLazyObject(oi);

       //read friendly string: [ETX][STX]ak[EXT]av[STX]bk[ETX]bv[STX]ck[ETX]cv[STX]dk[ETX]dv
        byte[] data = new byte[] {
            '\03',
            '\02', 'a', 'k', '\3', 'a', 'v',
            '\02', 'b', 'k', '\3', 'b', 'v',
            '\02', 'c', 'k', '\3', 'c', 'v',
            '\02', 'd', 'k', '\3', 'd', 'v'};
        TestLazyPrimitive.initLazyObject(b, data, 0, data.length);

        assertNull(b.getMapValueElement(new Text(""))); //{"" : null}
        assertEquals(new Text("av"), ((LazyString) b
            .getMapValueElement(new Text("ak"))).getWritableObject());
        assertNull(b.getMapValueElement(new Text("-1")));
        assertEquals(new Text("bv"), ((LazyString) b
            .getMapValueElement(new Text("bk"))).getWritableObject());
        assertEquals(new Text("cv"), ((LazyString) b
            .getMapValueElement(new Text("ck"))).getWritableObject());
        assertNull(b.getMapValueElement(new Text("-")));
        assertEquals(new Text("dv"), ((LazyString) b
            .getMapValueElement(new Text("dk"))).getWritableObject());
        assertEquals(4, b.getMapSize());
      }
    } catch(Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Test the LazyMap class.
   */
  public void testLazyMapWithDuplicateKeys() throws Throwable {
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
            'N', 1, '0', 2, '0', 1, '2', 2, 'a', 'b', 'c'};
        TestLazyPrimitive.initLazyObject(b, data, 0, data.length);

        assertEquals(new Text("def"), ((LazyString) b
            .getMapValueElement(new IntWritable(2))).getWritableObject());
        assertNull(b.getMapValueElement(new IntWritable(-1)));
        assertEquals(new Text("0"), ((LazyString) b
            .getMapValueElement(new IntWritable(0))).getWritableObject());
        assertNull(b.getMapValueElement(new IntWritable(12345)));

        assertEquals("{2:'def',-1:null,0:'0'}".replace('\'', '\"'),
            SerDeUtils.getJSONString(b, oi));

        assertEquals(3, b.getMapSize());
        assertEquals(3, b.getMap().size());
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
            'd', '#', '-', '1', '#', '0', '\t', '0', '#', '2', '\t', 'a', 'b', 'c'};
        TestLazyPrimitive.initLazyObject(b, data, 0, data.length);

        assertEquals(new Text("d\tf"), ((LazyString) b
            .getMapValueElement(new Text("2"))).getWritableObject());
        assertNull(b.getMapValueElement(new Text("-1")));
        assertEquals(new Text("0"), ((LazyString) b
            .getMapValueElement(new Text("0"))).getWritableObject());

        assertEquals("{'2':'d\\tf','-1':null,'0':'0'}"
            .replace('\'', '\"'), SerDeUtils.getJSONString(b, oi));

        assertEquals(3, b.getMapSize());
        assertEquals(3, b.getMap().size());
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
        assertEquals("{'a':null,'b':['',''],'c':{'':null},'d':':'}"
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

  /**
   * Test the LazyArray class with multiple levels of nesting
   */
  public void testLazyArrayNested() throws Throwable {
    for(int i = 2; i < EXTENDED_LEVEL_THRESHOLD; i++ ){
      testNestedinArrayAtLevelExtended(i, ObjectInspector.Category.LIST);
    }
  }

  /**
   * Test the LazyArray class with multiple levels of nesting
   */
  public void testLazyArrayNestedExceedLimit() throws Throwable {
    checkExtendedLimitExceeded(EXTENDED_LEVEL_THRESHOLD, ObjectInspector.Category.LIST);
  }

  private void checkExtendedLimitExceeded(int maxLevel, Category type) {
    boolean foundException = false;
    try {
      testNestedinArrayAtLevelExtended(maxLevel, type);
    }catch (SerDeException serdeEx){
      foundException = true;
    }
    assertTrue("Got exception for exceeding nesting limit" , foundException);
  }

  /**
   * Test the LazyArray class with multiple levels of nesting, when nesting
   * levels are not extended
   */
  public void testLazyArrayNestedExceedLimitNotExtended() throws Throwable {
    checkNotExtendedLimitExceeded(DEFAULT_LEVEL_THRESHOLD,
        ObjectInspector.Category.LIST);
  }

  /**
   * Test the LazyMap class with multiple levels of nesting, when nesting
   * levels are not extended
   */
  public void testLazyMapNestedExceedLimitNotExtended() throws Throwable {
    checkNotExtendedLimitExceeded(DEFAULT_LEVEL_THRESHOLD-1,
        ObjectInspector.Category.MAP);
  }

  /**
   * Test the LazyMap class with multiple levels of nesting, when nesting
   * levels are not extended
   */
  public void testLazyStructNestedExceedLimitNotExtended() throws Throwable {
    checkNotExtendedLimitExceeded(DEFAULT_LEVEL_THRESHOLD,
        ObjectInspector.Category.STRUCT);
  }

  /**
   * Test the LazyMap class with multiple levels of nesting, when nesting
   * levels are not extended
   */
  public void testLazyUnionNestedExceedLimitNotExtended() throws Throwable {
    checkNotExtendedLimitExceeded(DEFAULT_LEVEL_THRESHOLD,
        ObjectInspector.Category.UNION);
  }

  private void checkNotExtendedLimitExceeded(int maxLevel, Category type) {
    boolean foundException = false;
    try {
      testNestedinArrayAtLevel(maxLevel, type, new Properties());
    }catch (SerDeException serdeEx){
      foundException = true;
    }
    assertTrue("Expected exception for exceeding nesting limit" , foundException);
  }

  /**
   * Test the LazyMap class with multiple levels of nesting
   */
  public void testLazyMapNested() throws Throwable {
    //map max nesting level is one less because it uses an additional separator
    for(int i = 2; i < EXTENDED_LEVEL_THRESHOLD - 1; i++ ){
     testNestedinArrayAtLevelExtended(i, ObjectInspector.Category.MAP);
    }
  }

  /**
   * Test the LazyMap class with multiple levels of nesting
   */
  public void testLazyMapNestedExceedLimit() throws Throwable {
    //map max nesting level is one less because it uses an additional separator
    checkExtendedLimitExceeded(EXTENDED_LEVEL_THRESHOLD - 1, ObjectInspector.Category.MAP);
  }

  /**
   * Test the LazyUnion class with multiple levels of nesting
   */
  public void testLazyUnionNested() throws Throwable {
    for(int i = 2; i < EXTENDED_LEVEL_THRESHOLD; i++ ){
     testNestedinArrayAtLevelExtended(i, ObjectInspector.Category.UNION);
    }
  }

  /**
   * Test the LazyUnion class with multiple levels of nesting
   */
  public void testLazyUnionNestedExceedLimit() throws Throwable {
    checkExtendedLimitExceeded(EXTENDED_LEVEL_THRESHOLD, ObjectInspector.Category.UNION);
  }

  /**
   * Test the LazyStruct class with multiple levels of nesting
   */
  public void testLazyStructNested() throws Throwable {
    for(int i = 2; i < EXTENDED_LEVEL_THRESHOLD; i++ ){
     testNestedinArrayAtLevelExtended(i, ObjectInspector.Category.STRUCT);
    }
  }

  /**
   * Verify the serialized format for given type dtype, when it is nested in an
   * array with nestingLevel levels. with extended nesting enabled.
   * @param nestingLevel
   * @param dtype
   * @throws SerDeException
   */
  private void testNestedinArrayAtLevelExtended(int nestingLevel,
      ObjectInspector.Category dtype) throws SerDeException {
    Properties tableProp = new Properties();
    tableProp.setProperty(LazySerDeParameters.SERIALIZATION_EXTEND_NESTING_LEVELS, "true");
    testNestedinArrayAtLevel(nestingLevel, dtype, tableProp);
  }

  /**
   * Test the LazyStruct class with multiple levels of nesting
   */
  public void testLazyStructNestedExceedLimit() throws Throwable {
    checkExtendedLimitExceeded(EXTENDED_LEVEL_THRESHOLD, ObjectInspector.Category.STRUCT);
  }

  /**
   * @param nestingLevel
   * @param dtype
   * @param tableProp
   * @throws SerDeException
   */
  private void testNestedinArrayAtLevel(int nestingLevel,
      ObjectInspector.Category dtype, Properties tableProp) throws SerDeException {

    //create type with nestingLevel levels of nesting
    //set inner schema for dtype
    String inSchema = null;
    switch(dtype){
    case LIST:
      inSchema = "array<tinyint>";
      break;
    case MAP:
      inSchema = "map<string,int>";
      break;
    case STRUCT:
      inSchema = "struct<s:string,i:tinyint>";
      break;
    case UNION:
      inSchema = "uniontype<string,tinyint>";
      break;
    default :
        fail("type not supported by test case");
    }

    StringBuilder schema = new StringBuilder(inSchema);
    for(int i = 0; i < nestingLevel - 1; i++){
      schema.insert(0, "array<");
      schema.append(">");
    }
    System.err.println("Testing nesting level " + nestingLevel +
        ". Using schema " + schema);


    // Create the SerDe
    LazySimpleSerDe serDe = new LazySimpleSerDe();
    Configuration conf = new Configuration();
    tableProp.setProperty("columns", "narray");
    tableProp.setProperty("columns.types", schema.toString());
    SerDeUtils.initializeSerDe(serDe, conf, tableProp, null);
    LazySerDeParameters serdeParams = new LazySerDeParameters(conf, tableProp, LazySimpleSerDe.class.getName());
    
    //create the serialized string for type
    byte[] separators = serdeParams.getSeparators();
    System.err.println("Using separator " +  (char)separators[nestingLevel]);
    byte [] serializedRow = null;
    switch(dtype){
    case LIST:
      serializedRow = new byte[] {'8',separators[nestingLevel],'9'};
      break;
    case MAP:
      byte kvSep = separators[nestingLevel+1];
      byte kvPairSep = separators[nestingLevel];
      serializedRow = new byte[] {'1', kvSep, '1', kvPairSep, '2', kvSep, '2'};
      break;
    case STRUCT:
      serializedRow = new byte[] {'8',separators[nestingLevel],'9'};
      break;
    case UNION:
      serializedRow = new byte[] {'0',separators[nestingLevel],'9'};
      break;
    default :
        fail("type not supported by test case");
    }


    //create LazyStruct with serialized string with expected separators
    StructObjectInspector oi = (StructObjectInspector) serDe
        .getObjectInspector();
    LazyStruct struct = (LazyStruct) LazyFactory.createLazyObject(oi);

    TestLazyPrimitive.initLazyObject(struct, serializedRow, 0, serializedRow.length);


    //Get fields out of the lazy struct and check if they match expected
    // results
    //Get first level array
    LazyArray array = (LazyArray) struct.getField(0);

    //Peel off the n-1 levels to get to the underlying array
    for(int i = 0; i < nestingLevel - 2; i++){
      array = (LazyArray) array.getListElementObject(0);
    }

    //verify the serialized format for dtype
    switch(dtype){
    case LIST:
      LazyArray array1 = (LazyArray) array.getListElementObject(0);
      //check elements of the innermost array
      assertEquals(2, array1.getListLength());
      assertEquals(new ByteWritable((byte) 8), ((LazyByte) array1
          .getListElementObject(0)).getWritableObject());
      assertEquals(new ByteWritable((byte) 9), ((LazyByte) array1
          .getListElementObject(1)).getWritableObject());
      break;

    case MAP:
      LazyMap lazyMap = (LazyMap) array.getListElementObject(0);
      Map map = lazyMap.getMap();
      System.err.println(map);
      assertEquals(2, map.size());
      Iterator<Map.Entry<LazyString, LazyInteger>> it = map.entrySet().iterator();

      Entry<LazyString, LazyInteger> e1 = it.next();
      assertEquals(e1.getKey().getWritableObject(), new Text(new byte[]{'1'}) );
      assertEquals(e1.getValue().getWritableObject(), new IntWritable(1) );

      Entry<LazyString, LazyInteger> e2 = it.next();
      assertEquals(e2.getKey().getWritableObject(), new Text(new byte[]{'2'}) );
      assertEquals(e2.getValue().getWritableObject(), new IntWritable(2) );
      break;

    case STRUCT:
      LazyStruct innerStruct = (LazyStruct) array.getListElementObject(0);
      //check elements of the innermost struct
      assertEquals(2, innerStruct.getFieldsAsList().size());
      assertEquals(new Text(new byte[]{'8'}),
          ((LazyString) innerStruct.getField(0)).getWritableObject());
      assertEquals(new ByteWritable((byte) 9),
          ((LazyByte) innerStruct.getField(1)).getWritableObject());
      break;

    case UNION:
      LazyUnion lazyUnion = (LazyUnion) array.getListElementObject(0);
      //check elements of the innermost union
      assertEquals(new Text(new byte[]{'9'}),
          ((LazyString)lazyUnion.getField()).getWritableObject());
      break;


    default :
        fail("type not supported by test case");
    }

    //test serialization
    Text serializedText =
        (Text) serDe.serialize(struct.getObject(), serDe.getObjectInspector());
    org.junit.Assert.assertArrayEquals(serializedRow, serializedText.getBytes());

  }



}
