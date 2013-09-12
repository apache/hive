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
package org.apache.hive.hcatalog.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJsonSerDe extends TestCase {

  private static final Logger LOG = LoggerFactory.getLogger(TestJsonSerDe.class);

  public List<Pair<Properties, HCatRecord>> getData() {
    List<Pair<Properties, HCatRecord>> data = new ArrayList<Pair<Properties, HCatRecord>>();

    List<Object> rlist = new ArrayList<Object>(13);
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

    String typeString =
        "tinyint,smallint,int,bigint,double,float,string,string,"
            + "struct<a:string,b:string>,array<int>,map<smallint,string>,boolean,"
            + "array<struct<i1:int,i2:struct<ii1:array<int>,ii2:map<string,struct<iii1:int>>>>>";
    Properties props = new Properties();

    props.put(serdeConstants.LIST_COLUMNS, "ti,si,i,bi,d,f,s,n,r,l,m,b,c1");
    props.put(serdeConstants.LIST_COLUMN_TYPES, typeString);
//    props.put(Constants.SERIALIZATION_NULL_FORMAT, "\\N");
//    props.put(Constants.SERIALIZATION_FORMAT, "1");

    data.add(new Pair(props, new DefaultHCatRecord(rlist)));
    data.add(new Pair(props, new DefaultHCatRecord(nlist)));
    return data;
  }

  public void testRW() throws Exception {

    Configuration conf = new Configuration();

    for (Pair<Properties, HCatRecord> e : getData()) {
      Properties tblProps = e.first;
      HCatRecord r = e.second;

      HCatRecordSerDe hrsd = new HCatRecordSerDe();
      hrsd.initialize(conf, tblProps);

      JsonSerDe jsde = new JsonSerDe();
      jsde.initialize(conf, tblProps);

      LOG.info("ORIG:{}", r);

      Writable s = hrsd.serialize(r, hrsd.getObjectInspector());
      LOG.info("ONE:{}", s);

      Object o1 = hrsd.deserialize(s);
      assertTrue(HCatDataCheckUtil.recordsEqual(r, (HCatRecord) o1));

      Writable s2 = jsde.serialize(o1, hrsd.getObjectInspector());
      LOG.info("TWO:{}", s2);
      Object o2 = jsde.deserialize(s2);
      LOG.info("deserialized TWO : {} ", o2);

      assertTrue(HCatDataCheckUtil.recordsEqual(r, (HCatRecord) o2));
    }

  }

  public void testRobustRead() throws Exception {
    /**
     *  This test has been added to account for HCATALOG-436
     *  We write out columns with "internal column names" such
     *  as "_col0", but try to read with retular column names.
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
      wjsd.initialize(conf, internalTblProps);

      JsonSerDe rjsd = new JsonSerDe();
      rjsd.initialize(conf, tblProps);

      LOG.info("ORIG:{}", r);

      Writable s = wjsd.serialize(r, wjsd.getObjectInspector());
      LOG.info("ONE:{}", s);

      Object o1 = wjsd.deserialize(s);
      LOG.info("deserialized ONE : {} ", o1);

      Object o2 = rjsd.deserialize(s);
      LOG.info("deserialized TWO : {} ", o2);
      assertTrue(HCatDataCheckUtil.recordsEqual(r, (HCatRecord) o2));
    }

  }

  String getInternalNames(String columnNames) {
    if (columnNames == null) {
      return null;
    }
    if (columnNames.isEmpty()) {
      return "";
    }

    StringBuffer sb = new StringBuffer();
    int numStrings = columnNames.split(",").length;
    sb.append("_col0");
    for (int i = 1; i < numStrings; i++) {
      sb.append(",");
      sb.append(HiveConf.getColumnInternalName(i));
    }
    return sb.toString();
  }
}
