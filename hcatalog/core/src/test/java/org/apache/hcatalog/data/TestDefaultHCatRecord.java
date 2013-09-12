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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.data;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.data.schema.HCatSchemaUtils;

import junit.framework.Assert;
import junit.framework.TestCase;
/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.data.TestDefaultHCatRecord} instead
 */
public class TestDefaultHCatRecord extends TestCase {

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
      Assert.assertTrue(HCatDataCheckUtil.recordsEqual(recs[i], rec));
    }

    Assert.assertEquals(fInStream.available(), 0);
    fInStream.close();

  }

  public void testCompareTo() {
    HCatRecord[] recs = getHCatRecords();
    Assert.assertTrue(HCatDataCheckUtil.compareRecords(recs[0], recs[1]) == 0);
    Assert.assertTrue(HCatDataCheckUtil.compareRecords(recs[4], recs[5]) == 0);
  }

  public void testEqualsObject() {

    HCatRecord[] recs = getHCatRecords();
    Assert.assertTrue(HCatDataCheckUtil.recordsEqual(recs[0], recs[1]));
    Assert.assertTrue(HCatDataCheckUtil.recordsEqual(recs[4], recs[5]));
  }

  /**
   * Test get and set calls with type
   * @throws HCatException
   */
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


  private HCatRecord getGetSet2InpRec() {
    List<Object> rlist = new ArrayList<Object>();

    rlist.add(new byte[]{1, 2, 3});

    Map<Short, String> mapcol = new HashMap<Short, String>(3);
    mapcol.put(new Short("2"), "hcat is cool");
    mapcol.put(new Short("3"), "is it?");
    mapcol.put(new Short("4"), "or is it not?");
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
    rec_1.add(new Byte("123"));
    rec_1.add(new Short("456"));
    rec_1.add(new Integer(789));
    rec_1.add(new Long(1000L));
    rec_1.add(new Float(5.3F));
    rec_1.add(new Double(5.3D));
    rec_1.add(new Boolean(true));
    rec_1.add(new String("hcat and hadoop"));
    rec_1.add(null);
    rec_1.add("null");

    HCatRecord tup_1 = new DefaultHCatRecord(rec_1);

    List<Object> rec_2 = new ArrayList<Object>(8);
    rec_2.add(new Byte("123"));
    rec_2.add(new Short("456"));
    rec_2.add(new Integer(789));
    rec_2.add(new Long(1000L));
    rec_2.add(new Float(5.3F));
    rec_2.add(new Double(5.3D));
    rec_2.add(new Boolean(true));
    rec_2.add(new String("hcat and hadoop"));
    rec_2.add(null);
    rec_2.add("null");
    HCatRecord tup_2 = new DefaultHCatRecord(rec_2);

    List<Object> rec_3 = new ArrayList<Object>(10);
    rec_3.add(new Byte("123"));
    rec_3.add(new Short("456"));
    rec_3.add(new Integer(789));
    rec_3.add(new Long(1000L));
    rec_3.add(new Double(5.3D));
    rec_3.add(new String("hcat and hadoop"));
    rec_3.add(null);
    List<Integer> innerList = new ArrayList<Integer>();
    innerList.add(314);
    innerList.add(007);
    rec_3.add(innerList);
    Map<Short, String> map = new HashMap<Short, String>(3);
    map.put(new Short("2"), "hcat is cool");
    map.put(new Short("3"), "is it?");
    map.put(new Short("4"), "or is it not?");
    rec_3.add(map);

    HCatRecord tup_3 = new DefaultHCatRecord(rec_3);

    List<Object> rec_4 = new ArrayList<Object>(8);
    rec_4.add(new Byte("123"));
    rec_4.add(new Short("456"));
    rec_4.add(new Integer(789));
    rec_4.add(new Long(1000L));
    rec_4.add(new Double(5.3D));
    rec_4.add(new String("hcat and hadoop"));
    rec_4.add(null);
    rec_4.add("null");

    Map<Short, String> map2 = new HashMap<Short, String>(3);
    map2.put(new Short("2"), "hcat is cool");
    map2.put(new Short("3"), "is it?");
    map2.put(new Short("4"), "or is it not?");
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


    return new HCatRecord[]{tup_1, tup_2, tup_3, tup_4, tup_5, tup_6};

  }

  private Object getList() {
    return getStruct();
  }

  private Object getByteArray() {
    return new byte[]{1, 2, 3, 4};
  }

  private List<?> getStruct() {
    List<Object> struct = new ArrayList<Object>();
    struct.add(new Integer(1));
    struct.add(new String("x"));
    return struct;
  }
}
