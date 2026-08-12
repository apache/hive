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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;

import org.junit.Assert;
import junit.framework.TestCase;
import org.apache.pig.parser.AliasMasker;
import org.junit.Test;

/**
 * TestDefaultHCatRecord.
 */
public class TestDefaultHCatRecord {

  /**
   * test that we properly serialize/deserialize HCatRecordS
   * @throws IOException
   */
  @Test
  public void testRYW() throws IOException {

    File f = new File("binary.dat");
    f.delete();
    f.createNewFile();
    f.deleteOnExit();

    OutputStream fileOutStream = new FileOutputStream(f);
    DataOutput outStream = new DataOutputStream(fileOutStream);

    HCatRecord[] recs = getHCatRecords();
    for (int i = 0; i < recs.length; i++) {
      recs[i].write(outStream);
    }
    fileOutStream.flush();
    fileOutStream.close();

    InputStream fInStream = new FileInputStream(f);
    DataInput inpStream = new DataInputStream(fInStream);

    for (int i = 0; i < recs.length; i++) {
      HCatRecord rec = new DefaultHCatRecord();
      rec.readFields(inpStream);
      StringBuilder msg = new StringBuilder("recs[" + i + "]='" + recs[i] + "' rec='" + rec + "'");
      boolean isEqual = HCatDataCheckUtil.recordsEqual(recs[i], rec, msg);
      Assert.assertTrue(msg.toString(), isEqual);
    }

    Assert.assertEquals(fInStream.available(), 0);
    fInStream.close();

  }

  @Test
  public void testCompareTo() {
    HCatRecord[] recs = getHCatRecords();
    Assert.assertTrue(HCatDataCheckUtil.compareRecords(recs[0], recs[1]) == 0);
    Assert.assertTrue(HCatDataCheckUtil.compareRecords(recs[4], recs[5]) == 0);
  }

  @Test
  public void testEqualsObject() {

    HCatRecord[] recs = getHCatRecords();
    Assert.assertTrue(HCatDataCheckUtil.recordsEqual(recs[0], recs[1]));
    Assert.assertTrue(HCatDataCheckUtil.recordsEqual(recs[4], recs[5]));
  }

  /**
   * Test get and set calls with type
   * @throws HCatException
   */
  @Test
  public void testGetSetByType1() throws HCatException {
    HCatRecord inpRec = getHCatRecords()[0];
    HCatRecord newRec = new DefaultHCatRecord(inpRec.size());
    HCatSchema hsch =
        HCatSchemaUtils.getHCatSchema(
            "a:tinyint,b:smallint,c:int,d:bigint,e:float,f:double,g:boolean,h:string,i:binary,j:string");


    newRec.setByte("a", hsch, inpRec.getByte("a", hsch));
    newRec.setShort("b", hsch, inpRec.getShort("b", hsch));
    newRec.setInteger("c", hsch, inpRec.getInteger("c", hsch));
    newRec.setLong("d", hsch, inpRec.getLong("d", hsch));
    newRec.setFloat("e", hsch, inpRec.getFloat("e", hsch));
    newRec.setDouble("f", hsch, inpRec.getDouble("f", hsch));
    newRec.setBoolean("g", hsch, inpRec.getBoolean("g", hsch));
    newRec.setString("h", hsch, inpRec.getString("h", hsch));
    newRec.setByteArray("i", hsch, inpRec.getByteArray("i", hsch));
    newRec.setString("j", hsch, inpRec.getString("j", hsch));

    Assert.assertTrue(HCatDataCheckUtil.recordsEqual(newRec, inpRec));


  }

  /**
   * Test get and set calls with type
   * @throws HCatException
   */
  @Test
  public void testGetSetByType2() throws HCatException {
    HCatRecord inpRec = getGetSet2InpRec();

    HCatRecord newRec = new DefaultHCatRecord(inpRec.size());
    HCatSchema hsch =
        HCatSchemaUtils.getHCatSchema("a:binary,b:map<string,string>,c:array<int>,d:struct<i:int>");


    newRec.setByteArray("a", hsch, inpRec.getByteArray("a", hsch));
    newRec.setMap("b", hsch, inpRec.getMap("b", hsch));
    newRec.setList("c", hsch, inpRec.getList("c", hsch));
    newRec.setStruct("d", hsch, inpRec.getStruct("d", hsch));

    Assert.assertTrue(HCatDataCheckUtil.recordsEqual(newRec, inpRec));
  }

  /**
   * Test type specific get/set methods on HCatRecord types added in Hive 13
   * @throws HCatException
   */
  @Test
  public void testGetSetByType3() throws HCatException {
    HCatRecord inpRec = getHCat13TypesRecord();
    HCatRecord newRec = new DefaultHCatRecord(inpRec.size());
    HCatSchema hsch = HCatSchemaUtils.getHCatSchema(
            "a:decimal(5,2),b:char(10),c:varchar(20),d:date,e:timestamp");
    newRec.setDecimal("a", hsch, inpRec.getDecimal("a", hsch));
    newRec.setChar("b", hsch, inpRec.getChar("b", hsch));
    newRec.setVarchar("c", hsch, inpRec.getVarchar("c", hsch));
    newRec.setDate("d", hsch, inpRec.getDate("d", hsch));
    newRec.setTimestamp("e", hsch, inpRec.getTimestamp("e", hsch));
  }

  private HCatRecord getGetSet2InpRec() {
    List<Object> rlist = new ArrayList<Object>();

    rlist.add(new byte[]{1, 2, 3});

    Map<Short, String> mapcol = new HashMap<Short, String>(3);
    mapcol.put(Short.valueOf("2"), "hcat is cool");
    mapcol.put(Short.valueOf("3"), "is it?");
    mapcol.put(Short.valueOf("4"), "or is it not?");
    rlist.add(mapcol);

    List<Integer> listcol = new ArrayList<Integer>();
    listcol.add(314);
    listcol.add(007);
    rlist.add(listcol);//list
    rlist.add(listcol);//struct
    return new DefaultHCatRecord(rlist);
  }

  private HCatRecord[] getHCatRecords() {

    List<Object> rec_1 = new ArrayList<Object>(8);
    rec_1.add(Byte.valueOf("123"));
    rec_1.add(Short.valueOf("456"));
    rec_1.add(Integer.valueOf(789));
    rec_1.add(Long.valueOf(1000L));
    rec_1.add(Float.valueOf(5.3F));
    rec_1.add(Double.valueOf(5.3D));
    rec_1.add(Boolean.TRUE);
    rec_1.add("hcat and hadoop");
    rec_1.add(null);
    rec_1.add("null");

    HCatRecord tup_1 = new DefaultHCatRecord(rec_1);

    List<Object> rec_2 = new ArrayList<Object>(8);
    rec_2.add(Byte.valueOf("123"));
    rec_2.add(Short.valueOf("456"));
    rec_2.add(Integer.valueOf(789));
    rec_2.add(Long.valueOf(1000L));
    rec_2.add(Float.valueOf(5.3F));
    rec_2.add(Double.valueOf(5.3D));
    rec_2.add(Boolean.TRUE);
    rec_2.add("hcat and hadoop");
    rec_2.add(null);
    rec_2.add("null");
    HCatRecord tup_2 = new DefaultHCatRecord(rec_2);

    List<Object> rec_3 = new ArrayList<Object>(10);
    rec_3.add(Byte.valueOf("123"));
    rec_3.add(Short.valueOf("456"));
    rec_3.add(Integer.valueOf(789));
    rec_3.add(Long.valueOf(1000L));
    rec_3.add(Double.valueOf(5.3D));
    rec_3.add("hcat and hadoop");
    rec_3.add(null);
    List<Integer> innerList = new ArrayList<Integer>();
    innerList.add(314);
    innerList.add(007);
    rec_3.add(innerList);
    Map<Short, String> map = new HashMap<Short, String>(3);
    map.put(Short.valueOf("2"), "hcat is cool");
    map.put(Short.valueOf("3"), "is it?");
    map.put(Short.valueOf("4"), "or is it not?");
    rec_3.add(map);

    HCatRecord tup_3 = new DefaultHCatRecord(rec_3);

    List<Object> rec_4 = new ArrayList<Object>(8);
    rec_4.add(Byte.valueOf("123"));
    rec_4.add(Short.valueOf("456"));
    rec_4.add(Integer.valueOf(789));
    rec_4.add(Long.valueOf(1000L));
    rec_4.add(Double.valueOf(5.3D));
    rec_4.add("hcat and hadoop");
    rec_4.add(null);
    rec_4.add("null");

    Map<Short, String> map2 = new HashMap<Short, String>(3);
    map2.put(Short.valueOf("2"), "hcat is cool");
    map2.put(Short.valueOf("3"), "is it?");
    map2.put(Short.valueOf("4"), "or is it not?");
    rec_4.add(map2);
    List<Integer> innerList2 = new ArrayList<Integer>();
    innerList2.add(314);
    innerList2.add(007);
    rec_4.add(innerList2);
    HCatRecord tup_4 = new DefaultHCatRecord(rec_4);


    List<Object> rec_5 = new ArrayList<Object>(3);
    rec_5.add(getByteArray());
    rec_5.add(getStruct());
    rec_5.add(getList());
    HCatRecord tup_5 = new DefaultHCatRecord(rec_5);


    List<Object> rec_6 = new ArrayList<Object>(3);
    rec_6.add(getByteArray());
    rec_6.add(getStruct());
    rec_6.add(getList());
    HCatRecord tup_6 = new DefaultHCatRecord(rec_6);

    return new HCatRecord[]{tup_1, tup_2, tup_3, tup_4, tup_5, tup_6, getHCat13TypesRecord(),
            getHCat13TypesComplexRecord()};
  }
  private static HCatRecord getHCat13TypesRecord() {
    List<Object> rec_hcat13types = new ArrayList<Object>(5);
    rec_hcat13types.add(HiveDecimal.create(new BigDecimal("123.45")));//prec 5, scale 2
    rec_hcat13types.add(new HiveChar("hive_char", 10));
    rec_hcat13types.add(new HiveVarchar("hive_varchar", 20));
    rec_hcat13types.add(Date.valueOf("2014-01-06"));
    rec_hcat13types.add(Timestamp.ofEpochMilli(System.currentTimeMillis()));
    return new DefaultHCatRecord(rec_hcat13types);
  }
  private static HCatRecord getHCat13TypesComplexRecord() {
    List<Object> rec_hcat13ComplexTypes = new ArrayList<Object>();
    Map<HiveDecimal, String> m = new HashMap<HiveDecimal, String>();
    m.put(HiveDecimal.create(new BigDecimal("1234.12")), "1234.12");
    m.put(HiveDecimal.create(new BigDecimal("1234.13")), "1234.13");
    rec_hcat13ComplexTypes.add(m);

    Map<Timestamp, List<Object>> m2 = new HashMap<Timestamp, List<Object>>();
    List<Object> list = new ArrayList<Object>();
    list.add(Date.valueOf("2014-01-05"));
    list.add(new HashMap<HiveDecimal, String>(m));
    m2.put(Timestamp.ofEpochMilli(System.currentTimeMillis()), list);
    rec_hcat13ComplexTypes.add(m2);
    return new DefaultHCatRecord(rec_hcat13ComplexTypes);
  }

  private Object getList() {
    return getStruct();
  }

  private Object getByteArray() {
    return new byte[]{1, 2, 3, 4};
  }

  private List<?> getStruct() {
    List<Object> struct = new ArrayList<Object>();
    struct.add(Integer.valueOf(1));
    struct.add("x");
    return struct;
  }
}
