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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.data;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

public class TestJsonSerDe extends TestCase {

  private static final Logger LOG = LoggerFactory.getLogger(TestJsonSerDe.class);

  public List<Pair<Properties, HCatRecord>> getData() throws UnsupportedEncodingException {
    List<Pair<Properties, HCatRecord>> data = new ArrayList<Pair<Properties, HCatRecord>>();

    List<Object> rlist = new ArrayList<Object>(13);
    rlist.add(Byte.valueOf("123"));
    rlist.add(Short.valueOf("456"));
    rlist.add(Integer.valueOf(789));
    rlist.add(Long.valueOf(1000L));
    rlist.add(Double.valueOf(5.3D));
    rlist.add(Float.valueOf(2.39F));
    rlist.add("hcat\nand\nhadoop");
    rlist.add(null);

    List<Object> innerStruct = new ArrayList<Object>(2);
    innerStruct.add("abc");
    innerStruct.add("def");
    rlist.add(innerStruct);

    List<Integer> innerList = new ArrayList<Integer>();
    innerList.add(314);
    innerList.add(007);
    rlist.add(innerList);

    Map<Short, String> map = new HashMap<Short, String>(3);
    map.put(Short.valueOf("2"), "hcat is cool");
    map.put(Short.valueOf("3"), "is it?");
    map.put(Short.valueOf("4"), "or is it not?");
    rlist.add(map);

    rlist.add(Boolean.TRUE);

    List<Object> c1 = new ArrayList<Object>();
    List<Object> c1_1 = new ArrayList<Object>();
    c1_1.add(Integer.valueOf(12));
    List<Object> i2 = new ArrayList<Object>();
    List<Integer> ii1 = new ArrayList<Integer>();
    ii1.add(Integer.valueOf(13));
    ii1.add(Integer.valueOf(14));
    i2.add(ii1);
    Map<String, List<?>> ii2 = new HashMap<String, List<?>>();
    List<Integer> iii1 = new ArrayList<Integer>();
    iii1.add(Integer.valueOf(15));
    ii2.put("phew", iii1);
    i2.add(ii2);
    c1_1.add(i2);
    c1.add(c1_1);
    rlist.add(c1);
    rlist.add(HiveDecimal.create(new BigDecimal("123.45")));//prec 5, scale 2
    rlist.add(new HiveChar("hive\nchar", 10));
    rlist.add(new HiveVarchar("hive\nvarchar", 20));
    rlist.add(Date.valueOf("2014-01-07"));
    rlist.add(Timestamp.ofEpochMilli(System.currentTimeMillis()));
    rlist.add("hive\nbinary".getBytes("UTF-8"));

    List<Object> nlist = new ArrayList<Object>(13);
    nlist.add(null); // tinyint
    nlist.add(null); // smallint
    nlist.add(null); // int
    nlist.add(null); // bigint
    nlist.add(null); // double
    nlist.add(null); // float
    nlist.add(null); // string
    nlist.add(null); // string
    nlist.add(null); // struct
    nlist.add(null); // array
    nlist.add(null); // map
    nlist.add(null); // bool
    nlist.add(null); // complex
    nlist.add(null); //decimal(5,2)
    nlist.add(null); //char(10)
    nlist.add(null); //varchar(20)
    nlist.add(null); //date
    nlist.add(null); //timestamp
    nlist.add(null); //binary

    String typeString =
        "tinyint,smallint,int,bigint,double,float,string,string,"
            + "struct<a:string,b:string>,array<int>,map<smallint,string>,boolean,"
            + "array<struct<i1:int,i2:struct<ii1:array<int>,ii2:map<string,struct<iii1:int>>>>>," +
                "decimal(5,2),char(10),varchar(20),date,timestamp,binary";
    Properties props = new Properties();

    props.put(serdeConstants.LIST_COLUMNS, "ti,si,i,bi,d,f,s,n,r,l,m,b,c1,bd,hc,hvc,dt,ts,bin");
    props.put(serdeConstants.LIST_COLUMN_TYPES, typeString);
//    props.put(Constants.SERIALIZATION_NULL_FORMAT, "\\N");
//    props.put(Constants.SERIALIZATION_FORMAT, "1");

    data.add(new Pair<Properties, HCatRecord>(props, new DefaultHCatRecord(rlist)));
    data.add(new Pair<Properties, HCatRecord>(props, new DefaultHCatRecord(nlist)));
    return data;
  }

  public void testRW() throws Exception {

    Configuration conf = new Configuration();

    for (Pair<Properties, HCatRecord> e : getData()) {
      Properties tblProps = e.first;
      HCatRecord r = e.second;

      HCatRecordSerDe hrsd = new HCatRecordSerDe();
      SerDeUtils.initializeSerDe(hrsd, conf, tblProps, null);

      JsonSerDe jsde = new JsonSerDe();
      SerDeUtils.initializeSerDe(jsde, conf, tblProps, null);

      LOG.info("ORIG:{}", r);

      Writable s = hrsd.serialize(r, hrsd.getObjectInspector());
      LOG.info("ONE:{}", s);

      HCatRecord o1 = (HCatRecord) hrsd.deserialize(s);
      StringBuilder msg = new StringBuilder();
      boolean isEqual = HCatDataCheckUtil.recordsEqual(r, o1);
      assertTrue(msg.toString(), isEqual);

      Writable s2 = jsde.serialize(o1, hrsd.getObjectInspector());
      LOG.info("TWO:{}", s2);
      HCatRecord o2 = (HCatRecord) jsde.deserialize(s2);
      LOG.info("deserialized TWO : {} ", o2);
      msg.setLength(0);
      isEqual = HCatDataCheckUtil.recordsEqual(r, o2, msg);
      assertTrue(msg.toString(), isEqual);
    }

  }

  public void testRobustRead() throws Exception {
    /**
     *  This test has been added to account for HCATALOG-436
     *  We write out columns with "internal column names" such
     *  as "_col0", but try to read with regular column names.
     */

    Configuration conf = new Configuration();

    for (Pair<Properties, HCatRecord> e : getData()) {
      Properties tblProps = e.first;
      HCatRecord r = e.second;

      Properties internalTblProps = new Properties();
      for (Map.Entry pe : tblProps.entrySet()) {
        if (!pe.getKey().equals(serdeConstants.LIST_COLUMNS)) {
          internalTblProps.put(pe.getKey(), pe.getValue());
        } else {
          internalTblProps.put(pe.getKey(), getInternalNames((String) pe.getValue()));
        }
      }

      LOG.info("orig tbl props:{}", tblProps);
      LOG.info("modif tbl props:{}", internalTblProps);

      JsonSerDe wjsd = new JsonSerDe();
      SerDeUtils.initializeSerDe(wjsd, conf, internalTblProps, null);

      JsonSerDe rjsd = new JsonSerDe();
      SerDeUtils.initializeSerDe(rjsd, conf, tblProps, null);

      LOG.info("ORIG:{}", r);

      Writable s = wjsd.serialize(r, wjsd.getObjectInspector());
      LOG.info("ONE:{}", s);

      Object o1 = wjsd.deserialize(s);
      LOG.info("deserialized ONE : {} ", o1);

      Object o2 = rjsd.deserialize(s);
      LOG.info("deserialized TWO : {} ", o2);
      StringBuilder msg = new StringBuilder();
      boolean isEqual = HCatDataCheckUtil.recordsEqual(r, (HCatRecord) o2, msg);
      assertTrue(msg.toString(), isEqual);
    }

  }

  String getInternalNames(String columnNames) {
    if (columnNames == null) {
      return null;
    }
    if (columnNames.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    int numStrings = columnNames.split(",").length;
    sb.append("_col0");
    for (int i = 1; i < numStrings; i++) {
      sb.append(",");
      sb.append(HiveConf.getColumnInternalName(i));
    }
    return sb.toString();
  }

  /**
   * This test tests that our json deserialization is not too strict, as per HIVE-6166
   *
   * i.e, if our schema is "s:struct<a:int,b:string>,k:int", and we pass in
   * data that looks like : {
   *                            "x" : "abc" ,
   *                            "t" : {
   *                                "a" : "1",
   *                                "b" : "2",
   *                                "c" : [
   *                                    { "x" : 2 , "y" : 3 } ,
   *                                    { "x" : 3 , "y" : 2 }
   *                                ]
   *                            } ,
   *                            "s" : {
   *                                "a" : 2 ,
   *                                "b" : "blah",
   *                                "c": "woo"
   *                            }
   *                        }
   *
   * Then it should still work, and ignore the "x" and "t" field and "c" subfield of "s", and it
   * should read k as null.
   */
  public void testLooseJsonReadability() throws Exception {
    Configuration conf = new Configuration();
    Properties props = new Properties();

    props.put(serdeConstants.LIST_COLUMNS, "s,k");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "struct<a:int,b:string>,int");
    JsonSerDe rjsd = new JsonSerDe();
    SerDeUtils.initializeSerDe(rjsd, conf, props, null);

    Text jsonText = new Text("{ \"x\" : \"abc\" , "
        + " \"t\" : { \"a\":\"1\", \"b\":\"2\", \"c\":[ { \"x\":2 , \"y\":3 } , { \"x\":3 , \"y\":2 }] } ,"
        +"\"s\" : { \"a\" : 2 , \"b\" : \"blah\", \"c\": \"woo\" } }");
    List<Object> expected = new ArrayList<Object>();
    List<Object> inner = new ArrayList<Object>();
    inner.add(2);
    inner.add("blah");
    expected.add(inner);
    expected.add(null);
    HCatRecord expectedRecord = new DefaultHCatRecord(expected);

    HCatRecord r = (HCatRecord) rjsd.deserialize(jsonText);
    System.err.println("record : " + r.toString());

    assertTrue(HCatDataCheckUtil.recordsEqual(r, expectedRecord));

  }

  public void testUpperCaseKey() throws Exception {
    Configuration conf = new Configuration();
    Properties props = new Properties();

    props.put(serdeConstants.LIST_COLUMNS, "empid,name");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "int,string");
    JsonSerDe rjsd = new JsonSerDe();
    SerDeUtils.initializeSerDe(rjsd, conf, props, null);

    Text text1 = new Text("{ \"empId\" : 123, \"name\" : \"John\" } ");
    Text text2 = new Text("{ \"empId\" : 456, \"name\" : \"Jane\" } ");

    HCatRecord expected1 = new DefaultHCatRecord(Arrays.<Object>asList(123, "John"));
    HCatRecord expected2 = new DefaultHCatRecord(Arrays.<Object>asList(456, "Jane"));

    assertTrue(HCatDataCheckUtil.recordsEqual((HCatRecord)rjsd.deserialize(text1), expected1));
    assertTrue(HCatDataCheckUtil.recordsEqual((HCatRecord)rjsd.deserialize(text2), expected2));

  }

  private static HashMap<String, Integer> createHashMapStringInteger(Object...vals) {
    assertTrue(vals.length % 2 == 0);
    HashMap<String, Integer> retval = new HashMap<String, Integer>();
    for (int idx = 0; idx < vals.length; idx += 2) {
      retval.put((String) vals[idx], (Integer) vals[idx+1]);
    }
    return retval;
  }

  public void testMapValues() throws Exception {
    Configuration conf = new Configuration();
    Properties props = new Properties();

    props.put(serdeConstants.LIST_COLUMNS, "a,b");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "array<string>,map<string,int>");
    JsonSerDe rjsd = new JsonSerDe();
    SerDeUtils.initializeSerDe(rjsd, conf, props, null);

    Text text1 = new Text("{ \"a\":[\"aaa\"],\"b\":{\"bbb\":1}} ");
    Text text2 = new Text("{\"a\":[\"yyy\"],\"b\":{\"zzz\":123}}");
    Text text3 = new Text("{\"a\":[\"a\"],\"b\":{\"x\":11, \"y\": 22, \"z\": null}}");

    HCatRecord expected1 = new DefaultHCatRecord(Arrays.<Object>asList(
        Arrays.<String>asList("aaa"),
        createHashMapStringInteger("bbb", 1)));
    HCatRecord expected2 = new DefaultHCatRecord(Arrays.<Object>asList(
        Arrays.<String>asList("yyy"),
        createHashMapStringInteger("zzz", 123)));
    HCatRecord expected3 = new DefaultHCatRecord(Arrays.<Object>asList(
        Arrays.<String>asList("a"),
        createHashMapStringInteger("x", 11, "y", 22, "z", null)));

    assertTrue(HCatDataCheckUtil.recordsEqual((HCatRecord)rjsd.deserialize(text1), expected1));
    assertTrue(HCatDataCheckUtil.recordsEqual((HCatRecord)rjsd.deserialize(text2), expected2));
  }
}
