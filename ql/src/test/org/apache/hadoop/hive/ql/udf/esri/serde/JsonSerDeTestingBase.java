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
package org.apache.hadoop.hive.ql.udf.esri.serde;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.udf.esri.GeometryUtils;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;

import java.lang.reflect.Method;
import java.util.ArrayList;

public abstract class JsonSerDeTestingBase {

  protected void addWritable(ArrayList<Object> stuff, boolean item) {
    stuff.add(new BooleanWritable(item));
  }

  protected void addWritable(ArrayList<Object> stuff, byte item) {
    stuff.add(new ByteWritable(item));
  }

  protected void addWritable(ArrayList<Object> stuff, short item) {
    stuff.add(new ShortWritable(item));
  }

  protected void addWritable(ArrayList<Object> stuff, int item) {
    stuff.add(new IntWritable(item));
  }

  protected void addWritable(ArrayList<Object> stuff, long item) {
    stuff.add(new LongWritable(item));
  }

  protected void addWritable(ArrayList<Object> stuff, String item) {
    stuff.add(new Text(item));
  }

  protected void addWritable(ArrayList<Object> stuff, java.sql.Date item) {
    try {
      Class<?> dtClazz = Class.forName("org.apache.hadoop.hive.common.type.Date");
      Class<?> dwClazz = Class.forName("org.apache.hadoop.hive.serde2.io.DateWritableV2");
      Method dtSetImpl = dtClazz.getMethod("setTimeInMillis", long.class);
      Method dwSetImpl = dwClazz.getMethod("set", dtClazz);
      Object dtObj = dtClazz.getConstructor().newInstance();
      dtSetImpl.invoke(dtObj, item.getTime());
      Object dwObj = dwClazz.getConstructor().newInstance();
      dwSetImpl.invoke(dwObj, dtObj);
      stuff.add(dwObj);
    } catch (Exception exc) {
      stuff.add(new DateWritable(item));
    }
  }

  protected void addWritable(ArrayList<Object> stuff, java.sql.Timestamp item) {
    try {
      Class<?> ttClazz = Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
      Class<?> twClazz = Class.forName("org.apache.hadoop.hive.serde2.io.TimestampWritableV2");
      Method ttSetImpl = ttClazz.getMethod("setTimeInMillis", long.class);
      Method twSetImpl = twClazz.getMethod("set", ttClazz);
      Object ttObj = ttClazz.getConstructor().newInstance();
      ttSetImpl.invoke(ttObj, item.getTime());
      Object twObj = twClazz.getConstructor().newInstance();
      twSetImpl.invoke(twObj, ttObj);
      stuff.add(twObj);
    } catch (Exception exc) {
      stuff.add(new TimestampWritable(item));
    }
  }

  protected void addWritable(ArrayList<Object> stuff, Geometry geom) {
    addWritable(stuff, geom, null);
  }

  protected void addWritable(ArrayList<Object> stuff, MapGeometry geom) {
    addWritable(stuff, geom.getGeometry(), geom.getSpatialReference());
  }

  protected void addWritable(ArrayList<Object> stuff, Geometry geom, SpatialReference sref) {
    stuff.add(GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(geom, sref)));
  }

  protected void ckPoint(Point refPt, BytesWritable fieldData) {
    Assert.assertEquals(refPt, GeometryUtils.geometryFromEsriShape(fieldData).getEsriGeometry());
  }

  protected long epochFromWritable(Object dwHive) throws Exception {
    // Hive 0.12 and above
    Class<?> dwClazz = Class.forName("org.apache.hadoop.hive.serde2.io.DateWritable");
    Method dwGetImpl = dwClazz.getMethod("get");
    Class<?> twClazz = Class.forName("org.apache.hadoop.hive.serde2.io.TimestampWritable");
    Method twGetImpl = twClazz.getMethod("getTimestamp");
    if (dwHive.getClass() == dwClazz) {
      // Counter the time-zone adjustment done by DateWritable#daysToMillis .
      // What the product should do about that is another discussion.
      long epoch = ((java.util.Date) (dwGetImpl.invoke(dwHive))).getTime();
      return epoch + java.util.TimeZone.getDefault().getOffset(epoch);
    } else if (dwHive.getClass() == twClazz) {
      return ((java.sql.Timestamp) (twGetImpl.invoke(dwHive))).getTime();
    } else {
      // Hive 3.1 and above
      dwClazz = Class.forName("org.apache.hadoop.hive.serde2.io.DateWritableV2");
      twClazz = Class.forName("org.apache.hadoop.hive.serde2.io.TimestampWritableV2");
      if (dwHive.getClass() == dwClazz) {
        Class<?> dtClazz = Class.forName("org.apache.hadoop.hive.common.type.Date");
        Method dtGetImpl = dtClazz.getMethod("toEpochMilli");
        dwGetImpl = dwClazz.getMethod("get");
        // Object dtObj = dtClazz.getConstructor().newInstance();
        return ((java.lang.Long) (dtGetImpl.invoke(dwGetImpl.invoke(dwHive)))).longValue();
      } else {  // dwHive.getClass() == twClazz
        Class<?> ttClazz = Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
        Method ttGetImpl = ttClazz.getMethod("toEpochMilli");
        twGetImpl = twClazz.getMethod("getTimestamp");
        return ((java.lang.Long) (ttGetImpl.invoke(twGetImpl.invoke(dwHive)))).longValue();
      }
    }
  }  // epochFromWritable

  protected Object getField(String col, Object row, StructObjectInspector rowOI) {
    StructField f0 = rowOI.getStructFieldRef(col);
    return rowOI.getStructFieldData(row, f0);
  }

  protected Object runSerDe(Object stuff, AbstractSerDe jserde, StructObjectInspector rowOI) throws Exception {
    Writable jsw = jserde.serialize(stuff, rowOI);
    return jserde.deserialize(jsw);
  }

  protected String iso8601FromWritable(Object dtwHive) throws Exception {
    // Hive 0.12 and above
    Class<?> dwClazz = Class.forName("org.apache.hadoop.hive.serde2.io.DateWritable");
    Method dwGetImpl = dwClazz.getMethod("get");
    Class<?> twClazz = Class.forName("org.apache.hadoop.hive.serde2.io.TimestampWritable");
    Method twGetImpl = twClazz.getMethod("getTimestamp");
    if (dtwHive.getClass() == dwClazz) {
      java.util.Date localDay = (java.util.Date) (dwGetImpl.invoke(dtwHive));
      java.text.SimpleDateFormat dtFmt = new java.text.SimpleDateFormat("yyyy-MM-dd");
      dtFmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
      return dtFmt.format(localDay);
    } else if (dtwHive.getClass() == twClazz) {
      java.text.SimpleDateFormat dtFmt = new java.text.SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS");
      dtFmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
      java.sql.Timestamp localDt = (java.sql.Timestamp) (twGetImpl.invoke(dtwHive));
      // return localDt.toString();
      return dtFmt.format(localDt);
    } else {
      // Hive 3.1 and above
      dwClazz = Class.forName("org.apache.hadoop.hive.serde2.io.DateWritableV2");
      twClazz = Class.forName("org.apache.hadoop.hive.serde2.io.TimestampWritableV2");
      if (dtwHive.getClass() == dwClazz) {
        Class<?> dtClazz = Class.forName("org.apache.hadoop.hive.common.type.Date");
        Method dtGetImpl = dtClazz.getMethod("toString");
        dwGetImpl = dwClazz.getMethod("get");
        // Object dtObj = dtClazz.getConstructor().newInstance();
        return (String) (dtGetImpl.invoke(dwGetImpl.invoke(dtwHive)));
      } else {  // dtwHive.getClass() == twClazz
        Class<?> ttClazz = Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
        twGetImpl = twClazz.getMethod("toString");
        return (String) (twGetImpl.invoke(dtwHive));
      }
    }
  }  // epochFromWritable

}
