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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.data.TestHCatRecordSerDe} instead
 */
public class TestHCatRecordSerDe extends TestCase {

  private static final Logger LOG = LoggerFactory.getLogger(TestHCatRecordSerDe.class);

  public Map<Properties, HCatRecord> getData() {
    Map<Properties, HCatRecord> data = new HashMap<Properties, HCatRecord>();

    List<Object> rlist = new ArrayList<Object>(11);
    rlist.add(new Byte("123"));
    rlist.add(new Short("456"));
    rlist.add(new Integer(789));
    rlist.add(new Long(1000L));
    rlist.add(new Double(5.3D));
    rlist.add(new Float(2.39F));
    rlist.add(new String("hcat and hadoop"));
    rlist.add(null);

    List<Object> innerStruct = new ArrayList<Object>(2);
    innerStruct.add(new String("abc"));
    innerStruct.add(new String("def"));
    rlist.add(innerStruct);

    List<Integer> innerList = new ArrayList<Integer>();
    innerList.add(314);
    innerList.add(007);
    rlist.add(innerList);

    Map<Short, String> map = new HashMap<Short, String>(3);
    map.put(new Short("2"), "hcat is cool");
    map.put(new Short("3"), "is it?");
    map.put(new Short("4"), "or is it not?");
    rlist.add(map);

    rlist.add(new Boolean(true));

    List<Object> c1 = new ArrayList<Object>();
    List<Object> c1_1 = new ArrayList<Object>();
    c1_1.add(new Integer(12));
    List<Object> i2 = new ArrayList<Object>();
    List<Integer> ii1 = new ArrayList<Integer>();
    ii1.add(new Integer(13));
    ii1.add(new Integer(14));
    i2.add(ii1);
    Map<String, List<?>> ii2 = new HashMap<String, List<?>>();
    List<Integer> iii1 = new ArrayList<Integer>();
    iii1.add(new Integer(15));
    ii2.put("phew", iii1);
    i2.add(ii2);
    c1_1.add(i2);
    c1.add(c1_1);
    rlist.add(c1);
    List<Object> am = new ArrayList<Object>();
    Map<String, String> am_1 = new HashMap<String, String>();
    am_1.put("noo", "haha");
    am.add(am_1);
    rlist.add(am);
    List<Object> aa = new ArrayList<Object>();
    List<String> aa_1 = new ArrayList<String>();
    aa_1.add("bloo");
    aa_1.add("bwahaha");
    aa.add(aa_1);
    rlist.add(aa);

    String typeString =
        "tinyint,smallint,int,bigint,double,float,string,string,"
            + "struct<a:string,b:string>,array<int>,map<smallint,string>,boolean,"
            + "array<struct<i1:int,i2:struct<ii1:array<int>,ii2:map<string,struct<iii1:int>>>>>,"
            + "array<map<string,string>>,array<array<string>>";
    Properties props = new Properties();

    props.put(serdeConstants.LIST_COLUMNS, "ti,si,i,bi,d,f,s,n,r,l,m,b,c1,am,aa");
    props.put(serdeConstants.LIST_COLUMN_TYPES, typeString);
//    props.put(Constants.SERIALIZATION_NULL_FORMAT, "\\N");
//    props.put(Constants.SERIALIZATION_FORMAT, "1");

    data.put(props, new DefaultHCatRecord(rlist));
    return data;
  }

  public void testRW() throws Exception {

    Configuration conf = new Configuration();

    for (Entry<Properties, HCatRecord> e : getData().entrySet()) {
      Properties tblProps = e.getKey();
      HCatRecord r = e.getValue();

      HCatRecordSerDe hrsd = new HCatRecordSerDe();
      hrsd.initialize(conf, tblProps);

      LOG.info("ORIG: {}", r);

      Writable s = hrsd.serialize(r, hrsd.getObjectInspector());
      LOG.info("ONE: {}", s);

      HCatRecord r2 = (HCatRecord) hrsd.deserialize(s);
      Assert.assertTrue(HCatDataCheckUtil.recordsEqual(r, r2));

      // If it went through correctly, then s is also a HCatRecord,
      // and also equal to the above, and a deepcopy, and this holds
      // through for multiple levels more of serialization as well.

      Writable s2 = hrsd.serialize(s, hrsd.getObjectInspector());
      LOG.info("TWO: {}", s2);
      Assert.assertTrue(HCatDataCheckUtil.recordsEqual(r, (HCatRecord) s));
      Assert.assertTrue(HCatDataCheckUtil.recordsEqual(r, (HCatRecord) s2));

      // serialize using another serde, and read out that object repr.
      LazySimpleSerDe testSD = new LazySimpleSerDe();
      testSD.initialize(conf, tblProps);

      Writable s3 = testSD.serialize(s, hrsd.getObjectInspector());
      LOG.info("THREE: {}", s3);
      Object o3 = testSD.deserialize(s3);
      Assert.assertFalse(r.getClass().equals(o3.getClass()));

      // then serialize again using hrsd, and compare results
      HCatRecord s4 = (HCatRecord) hrsd.serialize(o3, testSD.getObjectInspector());
      LOG.info("FOUR: {}", s4);

      // Test LazyHCatRecord init and read
      LazyHCatRecord s5 = new LazyHCatRecord(o3, testSD.getObjectInspector());
      LOG.info("FIVE: {}", s5);

      LazyHCatRecord s6 = new LazyHCatRecord(s4, hrsd.getObjectInspector());
      LOG.info("SIX: {}", s6);

    }

  }

}
