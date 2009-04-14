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

import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.util.HashMap;

import junit.framework.TestCase;

public class TestLazyArrayMapStruct extends TestCase {

  /**
   * Test the LazyArray class.
   */
  public void testLazyArray() throws Throwable {
    try {
      // Array of Byte
      Text nullSequence = new Text("\\N");
      LazyArray b = (LazyArray)LazyFactory.createLazyObject(TypeInfoUtils.getTypeInfoFromTypeString("array<tinyint>"));
      byte[] data = new byte[]{'-', '1', 1, '\\', 'N', 1, '8'};
      TestLazyPrimitive.initLazyObject(b, data, 0, data.length);
      
      assertNull(b.getListElementObject(-1, (byte)1, nullSequence));
      assertEquals(new ByteWritable((byte)-1), b.getListElementObject(0, (byte)1, nullSequence));
      assertNull(b.getListElementObject(1, (byte)1, nullSequence));
      assertEquals(new ByteWritable((byte)8), b.getListElementObject(2, (byte)1, nullSequence));
      assertNull(b.getListElementObject(3, (byte)1, nullSequence));
      assertEquals(
          Arrays.asList(new ByteWritable[]{new ByteWritable((byte)-1), null, new ByteWritable((byte)8)}),
          b.getList((byte)1, nullSequence));
      
      // Array of String
      b = (LazyArray)LazyFactory.createLazyObject(TypeInfoUtils.getTypeInfoFromTypeString("array<string>"));
      data = new byte[]{'a', 'b', '\t', 'c', '\t', '\\', 'N', '\t', '\t', 'd'};
      // Note: the first and last element of the byte[] are NOT used
      TestLazyPrimitive.initLazyObject(b, data, 1, data.length - 2);
      assertNull(b.getListElementObject(-1, (byte)'\t', nullSequence));
      assertEquals(new Text("b"), b.getListElementObject(0, (byte)'\t', nullSequence));
      assertEquals(new Text("c"), b.getListElementObject(1, (byte)'\t', nullSequence));
      assertNull(b.getListElementObject(2, (byte)'\t', nullSequence));
      assertEquals(new Text(""), b.getListElementObject(3, (byte)'\t', nullSequence));
      assertEquals(new Text(""), b.getListElementObject(4, (byte)'\t', nullSequence));
      assertNull(b.getListElementObject(5, (byte)'\t', nullSequence));
      assertEquals(Arrays.asList(new Text[]{new Text("b"), new Text("c"), null, new Text(""), new Text("")}),
          b.getList((byte)'\t', nullSequence));
      
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
        LazyMap b = (LazyMap)LazyFactory.createLazyObject(TypeInfoUtils.getTypeInfoFromTypeString("map<int,string>"));
        byte[] data = new byte[]{'2', 2, 'd', 'e', 'f', 1, '-', '1', 2, '\\', 'N', 1, '0', 2, '0', 1, '8', 2, 'a', 'b', 'c'};
        TestLazyPrimitive.initLazyObject(b, data, 0, data.length);
        
        assertEquals(new Text("def"), b.getMapValueElement((byte)1, (byte)2, nullSequence, new IntWritable(2)));
        assertNull(b.getMapValueElement((byte)1, (byte)2, nullSequence, Integer.valueOf(-1)));
        assertEquals(new Text("0"), b.getMapValueElement((byte)1, (byte)2, nullSequence, new IntWritable(0)));
        assertEquals(new Text("abc"), b.getMapValueElement((byte)1, (byte)2, nullSequence, new IntWritable(8)));
        assertNull(b.getMapValueElement((byte)1, (byte)2, nullSequence, new IntWritable(12345)));
        
        HashMap<IntWritable, Text> r = new HashMap<IntWritable, Text>();
        r.put(new IntWritable(2), new Text("def"));
        r.put(new IntWritable(-1), null);
        r.put(new IntWritable(0), new Text("0"));
        r.put(new IntWritable(8), new Text("abc"));
        assertEquals(r, b.getMap((byte)1, (byte)2, nullSequence));
      }
      
      {
        // Map of String to String
        Text nullSequence = new Text("\\N");
        LazyMap b = (LazyMap)LazyFactory.createLazyObject(TypeInfoUtils.getTypeInfoFromTypeString("map<string,string>"));
        byte[] data = new byte[]{'2', '\t', 'd', '\t', 'f', '#', '2', '\t', 'd', '#', '-', '1', '#', '0', '\t', '0', '#', '8', '\t', 'a', 'b', 'c'};
        TestLazyPrimitive.initLazyObject(b, data, 0, data.length);
        
        assertEquals(new Text("d\tf"), b.getMapValueElement((byte)'#', (byte)'\t', nullSequence, new Text("2")));
        assertNull(b.getMapValueElement((byte)'#', (byte)'\t', nullSequence, new Text("-1")));
        assertEquals(new Text("0"), b.getMapValueElement((byte)'#', (byte)'\t', nullSequence, new Text("0")));
        assertEquals(new Text("abc"), b.getMapValueElement((byte)'#', (byte)'\t', nullSequence, new Text("8")));
        assertNull(b.getMapValueElement((byte)'#', (byte)'\t', nullSequence, new Text("-")));
        
        HashMap<Text,Text> r = new HashMap<Text, Text>();
        r.put(new Text("2"), new Text("d\tf"));
        r.put(new Text("-1"), null);
        r.put(new Text("0"), new Text("0"));
        r.put(new Text("8"), new Text("abc"));
        assertEquals(r, b.getMap((byte)1, (byte)2, nullSequence));
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
        ArrayList<TypeInfo> fieldTypeInfos = 
          TypeInfoUtils.getTypeInfosFromTypeString("int,array<string>,map<string,string>,string");
        List<String> fieldNames = Arrays.asList(new String[]{"a", "b", "c", "d"});
        TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
        
        Text nullSequence = new Text("\\N");
        
        LazyStruct o = (LazyStruct)LazyFactory.createLazyObject(rowTypeInfo);
        ObjectInspector oi = LazyFactory.createLazyStructInspector(Arrays.asList(new String[]{"a","b","c","d"}),
            fieldTypeInfos, new byte[] {' ', ':', '='}, nullSequence, false);
        
        Text data;
        
        data = new Text("123 a:b:c d=e:f=g hi");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data.getLength());
        assertEquals("{'a':123,'b':['a','b','c'],'c':{'d':'e','f':'g'},'d':'hi'}".replace("'", "\""),
            SerDeUtils.getJSONString(o, oi));

        data = new Text("123 \\N d=e:f=g \\N");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data.getLength());
        assertEquals("{'a':123,'b':null,'c':{'d':'e','f':'g'},'d':null}".replace("'", "\""),
            SerDeUtils.getJSONString(o, oi));

        data = new Text("\\N a d=\\N:f=g:h no tail");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data.getLength());
        assertEquals("{'a':null,'b':['a'],'c':{'d':null,'f':'g','h':null},'d':'no'}".replace("'", "\""),
            SerDeUtils.getJSONString(o, oi));

        data = new Text("\\N :a:: \\N no tail");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data.getLength());
        assertEquals("{'a':null,'b':['','a','',''],'c':null,'d':'no'}".replace("'", "\""),
            SerDeUtils.getJSONString(o, oi));

        data = new Text("123   ");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data.getLength());
        assertEquals("{'a':123,'b':[],'c':{},'d':''}".replace("'", "\""),
            SerDeUtils.getJSONString(o, oi));

        data = new Text(": : : :");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data.getLength());
        assertEquals("{'a':null,'b':['',''],'c':{'':null},'d':':'}".replace("'", "\""),
            SerDeUtils.getJSONString(o, oi));

        data = new Text("= = = =");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data.getLength());
        assertEquals("{'a':null,'b':['='],'c':{'':''},'d':'='}".replace("'", "\""),
            SerDeUtils.getJSONString(o, oi));
        
        // test LastColumnTakesRest
        oi = LazyFactory.createLazyStructInspector(Arrays.asList(new String[]{"a","b","c","d"}),
            fieldTypeInfos, new byte[] {' ', ':', '='}, nullSequence, true);
        data = new Text("\\N a d=\\N:f=g:h has tail");
        TestLazyPrimitive.initLazyObject(o, data.getBytes(), 0, data.getLength());
        assertEquals("{'a':null,'b':['a'],'c':{'d':null,'f':'g','h':null},'d':'has tail'}".replace("'", "\""),
            SerDeUtils.getJSONString(o, oi));
      }
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
  
}
