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
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility functions
 */
class HBaseUtils {

  final static Charset ENCODING = StandardCharsets.UTF_8;
  final static char KEY_SEPARATOR = ':';

  static final private Log LOG = LogFactory.getLog(HBaseUtils.class.getName());

  /**
   * Build a key for an object in hbase
   * @param components
   * @return
   */
  static byte[] buildKey(String... components) {
    return buildKey(false, components);
  }

  static byte[] buildKeyWithTrailingSeparator(String... components) {
    return buildKey(true, components);
  }

  private static byte[] buildKey(boolean trailingSeparator, String... components) {
    String protoKey = StringUtils.join(components, KEY_SEPARATOR);
    if (trailingSeparator) protoKey += KEY_SEPARATOR;
    return protoKey.getBytes(ENCODING);
  }

  static byte[] serialize(Writable writable) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    writable.write(dos);
    return baos.toByteArray();
  }

  static <T extends Writable> void deserialize(T instance, byte[] bytes) throws IOException {
    DataInput in = new DataInputStream(new ByteArrayInputStream(bytes));
    instance.readFields(in);
  }

  static void writeStr(DataOutput out, String str) throws IOException {
    if (str == null || str.length() == 0) {
      out.writeInt(0);
      return;
    } else {
      out.writeInt(str.length());
      out.write(str.getBytes(), 0, str.length());
    }
  }

  static String readStr(DataInput in) throws IOException {
    int len = in.readInt();
    if (len == 0) {
      return new String();
    } else {
      byte[] b = new byte[len];
      in.readFully(b, 0, len);
      return new String(b);
    }
  }

  static void writeByteArray(DataOutput out, byte[] b) throws IOException {
    if (b == null || b.length == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(b.length);
      out.write(b, 0, b.length);
    }
  }

  static byte[] readByteArray(DataInput in) throws IOException {
    int len = in.readInt();
    if (len == 0) {
      return new byte[0];
    } else {
      byte[] b = new byte[len];
      in.readFully(b, 0, len);
      return b;
    }
  }

  static void writeDecimal(DataOutput out, Decimal val) throws IOException {
    HBaseUtils.writeByteArray(out, val.getUnscaled());
    out.writeShort(val.getScale());
  }

  static Decimal readDecimal(DataInput in) throws IOException {
    Decimal d = new Decimal();
    d.setUnscaled(HBaseUtils.readByteArray(in));
    d.setScale(in.readShort());
    return d;
  }

  static Map<String, String> readStrStrMap(DataInput in) throws IOException {
    int sz = in.readInt();
    if (sz == 0) {
      return new HashMap<String, String>();
    } else {
      Map<String, String> m = new HashMap<String, String>(sz);
      for (int i = 0; i < sz; i++) {
        m.put(readStr(in), readStr(in));
      }
      return m;
    }
  }


  static void writeStrStrMap(DataOutput out, Map<String, String> map) throws IOException {
    if (map == null || map.size() == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(map.size());
      for (Map.Entry<String, String> e : map.entrySet()) {
        writeStr(out, e.getKey());
        writeStr(out, e.getValue());
      }
    }
  }

  static Map<List<String>, String> readStrListStrMap(DataInput in) throws IOException {
    int sz = in.readInt();
    if (sz == 0) {
      return new HashMap<List<String>, String>();
    } else {
      Map<List<String>, String> m = new HashMap<List<String>, String>(sz);
      for (int i = 0; i < sz; i++) {
        m.put(readStrList(in), readStr(in));
      }
      return m;
    }
  }


  static void writeStrListStrMap(DataOutput out, Map<List<String>, String> map) throws IOException {
    if (map == null || map.size() == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(map.size());
      for (Map.Entry<List<String>, String> e : map.entrySet()) {
        writeStrList(out, e.getKey());
        writeStr(out, e.getValue());
      }
    }
  }

  static void writeStrList(DataOutput out, List<String> list) throws IOException {
    if (list == null || list.size() == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(list.size());
      for (String val : list) {
        writeStr(out, val);
      }
    }
  }

  static List<String> readStrList(DataInput in) throws IOException {
    int sz = in.readInt();
    if (sz == 0) {
      return new ArrayList<String>();
    } else {
      List<String> list = new ArrayList<String>(sz);
      for (int i = 0; i < sz; i++) {
        list.add(readStr(in));
      }
      return list;
    }
  }

  static void writeWritableList(DataOutput out, List<? extends Writable> list) throws IOException {
    if (list == null || list.size() == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(list.size());
      for (Writable val : list) {
        val.write(out);
      }
    }
  }

  static <T extends Writable> List<T> readWritableList(DataInput in, Class<T> clazz)
      throws IOException {
    int sz = in.readInt();
    if (sz == 0) {
      return new ArrayList<T>();
    } else {
      List<T> list = new ArrayList<T>(sz);
      for (int i = 0; i < sz; i++) {
        try {
          T instance = clazz.newInstance();
          instance.readFields(in);
          list.add(instance);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return list;
    }
  }

  static void writeStrListList(DataOutput out, List<List<String>> list) throws IOException {
    if (list == null || list.size() == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(list.size());
      for (List<String> vals : list) {
        writeStrList(out, vals);
      }
    }
  }

  static List<List<String>> readStrListList(DataInput in) throws IOException {
    int sz = in.readInt();
    if (sz == 0) {
      return new ArrayList<List<String>>();
    } else {
      List<List<String>> list = new ArrayList<List<String>>(sz);
      for (int i = 0; i < sz; i++) {
        list.add(readStrList(in));
      }
      return list;
    }
  }
  static List<FieldSchema> readFieldSchemaList(DataInput in) throws IOException {
    int sz = in.readInt();
    if (sz == 0) {
      return new ArrayList<FieldSchema>();
    } else {
      List<FieldSchema> schemas = new ArrayList<FieldSchema>(sz);
      for (int i = 0; i < sz; i++) {
        schemas.add(new FieldSchema(readStr(in), readStr(in), readStr(in)));
      }
      return schemas;
    }
  }

  static void writeFieldSchemaList(DataOutput out, List<FieldSchema> fields) throws IOException {
    if (fields == null || fields.size() == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(fields.size());
      for (FieldSchema field : fields) {
        writeStr(out, field.getName());
        writeStr(out, field.getType());
        writeStr(out, field.getComment());
      }
    }
  }

  static List<Order> readOrderList(DataInput in) throws IOException {
    int sz = in.readInt();
    if (sz == 0) {
      return new ArrayList<Order>();
    } else {
      List<Order> orderList = new ArrayList<Order>(sz);
      for (int i = 0; i < sz; i++) {
        orderList.add(new Order(readStr(in), in.readInt()));
      }
      return orderList;
    }
  }

  static void writeOrderList(DataOutput out, List<Order> orderList) throws IOException {
    if (orderList == null || orderList.size() == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(orderList.size());
      for (Order order : orderList) {
        writeStr(out, order.getCol());
        out.writeInt(order.getOrder());
      }
    }
  }

  static PrincipalPrivilegeSet readPrivileges(DataInput in) throws IOException {
    if (in.readBoolean()) {
      PrincipalPrivilegeSet pps = new PrincipalPrivilegeSet();
      pps.setUserPrivileges(readPrivilege(in));
      pps.setGroupPrivileges(readPrivilege(in));
      pps.setRolePrivileges(readPrivilege(in));
      return pps;
    } else {
      return new PrincipalPrivilegeSet();
    }

  }

  private static Map<String, List<PrivilegeGrantInfo>> readPrivilege(DataInput in)
      throws IOException {
    int sz = in.readInt();
    if (sz == 0) {
      return new HashMap<String, List<PrivilegeGrantInfo>>();
    } else {
      Map<String, List<PrivilegeGrantInfo>> priv =
          new HashMap<String, List<PrivilegeGrantInfo>>(sz);
      for (int i = 0; i < sz; i++) {
        String key = readStr(in);
        int numGrants = in.readInt();
        if (numGrants == 0) {
          priv.put(key, new ArrayList<PrivilegeGrantInfo>());
        } else {
          for (int j = 0; j < numGrants; j++) {
            PrivilegeGrantInfo pgi = new PrivilegeGrantInfo();
            pgi.setPrivilege(readStr(in));
            pgi.setCreateTime(in.readInt());
            pgi.setGrantor(readStr(in));
            pgi.setGrantorType(PrincipalType.findByValue(in.readInt()));
            pgi.setGrantOption(in.readBoolean());
          }
        }
      }
      return priv;
    }
  }

  static void writePrivileges(DataOutput out, PrincipalPrivilegeSet privs) throws IOException {
    if (privs == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      writePrivilege(out, privs.getUserPrivileges());
      writePrivilege(out, privs.getGroupPrivileges());
      writePrivilege(out, privs.getRolePrivileges());
    }
  }

  private static void writePrivilege(DataOutput out, Map<String,List<PrivilegeGrantInfo>> priv)
      throws IOException {
    if (priv == null || priv.size() == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(priv.size());
      for (Map.Entry<String, List<PrivilegeGrantInfo>> e : priv.entrySet()) {
        writeStr(out, e.getKey());
        List<PrivilegeGrantInfo> grants = e.getValue();
        if (grants == null || grants.size() == 0) {
          out.writeInt(0);
        } else {
          out.writeInt(grants.size());
          for (PrivilegeGrantInfo grant : grants) {
            writeStr(out, grant.getPrivilege());
            out.writeInt(grant.getCreateTime());
            writeStr(out, grant.getGrantor());
            out.writeInt(grant.getGrantorType().getValue());
            out.writeBoolean(grant.isGrantOption());
          }
        }
      }
    }
  }

  static void writePrincipalType(DataOutput out, PrincipalType pt) throws IOException {
    if (pt == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(pt.getValue());
    }
  }

  static PrincipalType readPrincipalType(DataInput in) throws IOException {
    return (in.readBoolean()) ? PrincipalType.findByValue(in.readInt()) : null;
  }

  static void writeSkewedInfo(DataOutput out, SkewedInfo skew) throws IOException {
    if (skew == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      writeStrList(out, skew.getSkewedColNames());
      writeStrListList(out, skew.getSkewedColValues());
      writeStrListStrMap(out, skew.getSkewedColValueLocationMaps());
    }
  }

  static SkewedInfo readSkewedInfo(DataInput in) throws IOException {
    if (in.readBoolean()) {
      SkewedInfo skew = new SkewedInfo();
      skew.setSkewedColNames(readStrList(in));
      skew.setSkewedColValues(readStrListList(in));
      skew.setSkewedColValueLocationMaps(readStrListStrMap(in));
      return skew;
    } else {
      return new SkewedInfo(new ArrayList<String>(), new ArrayList<List<String>>(),
          new HashMap<List<String>, String>());
    }
  }

  static byte[] serializeStorageDescriptor(StorageDescriptor sd) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    writeFieldSchemaList(dos, sd.getCols());
    writeStr(dos, sd.getInputFormat());
    writeStr(dos, sd.getOutputFormat());
    dos.writeBoolean(sd.isCompressed());
    dos.writeInt(sd.getNumBuckets());
    writeStr(dos, sd.getSerdeInfo().getName());
    writeStr(dos, sd.getSerdeInfo().getSerializationLib());
    writeStrStrMap(dos, sd.getSerdeInfo().getParameters());
    writeStrList(dos, sd.getBucketCols());
    writeOrderList(dos, sd.getSortCols());
    writeSkewedInfo(dos, sd.getSkewedInfo());
    dos.writeBoolean(sd.isStoredAsSubDirectories());
    return baos.toByteArray();
  }

  static void deserializeStorageDescriptor(StorageDescriptor sd, byte[] bytes)
      throws IOException {
    DataInput in = new DataInputStream(new ByteArrayInputStream(bytes));
    sd.setCols(readFieldSchemaList(in));
    sd.setInputFormat(readStr(in));
    sd.setOutputFormat(readStr(in));
    sd.setCompressed(in.readBoolean());
    sd.setNumBuckets(in.readInt());
    SerDeInfo serde = new SerDeInfo(readStr(in), readStr(in), readStrStrMap(in));
    sd.setSerdeInfo(serde);
    sd.setBucketCols(readStrList(in));
    sd.setSortCols(readOrderList(in));
    sd.setSkewedInfo(readSkewedInfo(in));
    sd.setStoredAsSubDirectories(in.readBoolean());
  }

  static byte[] serializeStatsForOneColumn(ColumnStatistics stats, ColumnStatisticsObj obj)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeLong(stats.getStatsDesc().getLastAnalyzed());
    HBaseUtils.writeStr(dos, obj.getColType());
    ColumnStatisticsData colData = obj.getStatsData();
    HBaseUtils.writeStr(dos, colData.getSetField().toString());
    switch (colData.getSetField()) {
      case BOOLEAN_STATS:
        BooleanColumnStatsData boolData = colData.getBooleanStats();
        dos.writeLong(boolData.getNumTrues());
        dos.writeLong(boolData.getNumFalses());
        dos.writeLong(boolData.getNumNulls());
        break;

      case LONG_STATS:
        LongColumnStatsData longData = colData.getLongStats();
        dos.writeLong(longData.getLowValue());
        dos.writeLong(longData.getHighValue());
        dos.writeLong(longData.getNumNulls());
        dos.writeLong(longData.getNumDVs());
        break;

      case DOUBLE_STATS:
        DoubleColumnStatsData doubleData = colData.getDoubleStats();
        dos.writeDouble(doubleData.getLowValue());
        dos.writeDouble(doubleData.getHighValue());
        dos.writeLong(doubleData.getNumNulls());
        dos.writeLong(doubleData.getNumDVs());
        break;

      case STRING_STATS:
        StringColumnStatsData stringData = colData.getStringStats();
        dos.writeLong(stringData.getMaxColLen());
        dos.writeDouble(stringData.getAvgColLen());
        dos.writeLong(stringData.getNumNulls());
        dos.writeLong(stringData.getNumDVs());
        break;

      case BINARY_STATS:
        BinaryColumnStatsData binaryData = colData.getBinaryStats();
        dos.writeLong(binaryData.getMaxColLen());
        dos.writeDouble(binaryData.getAvgColLen());
        dos.writeLong(binaryData.getNumNulls());
        break;

      case DECIMAL_STATS:
        DecimalColumnStatsData decimalData = colData.getDecimalStats();
        writeDecimal(dos, decimalData.getHighValue());
        writeDecimal(dos, decimalData.getLowValue());
        dos.writeLong(decimalData.getNumNulls());
        dos.writeLong(decimalData.getNumDVs());
        break;

      default:
        throw new RuntimeException("Woh, bad.  Unknown stats type!");
    }
    return baos.toByteArray();
  }

  static ColumnStatisticsObj deserializeStatsForOneColumn(ColumnStatistics stats,
                                                          byte[] bytes) throws IOException {
    DataInput in = new DataInputStream(new ByteArrayInputStream(bytes));
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    long lastAnalyzed = in.readLong();
    stats.getStatsDesc().setLastAnalyzed(
        Math.max(lastAnalyzed, stats.getStatsDesc().getLastAnalyzed()));
    obj.setColType(HBaseUtils.readStr(in));

    ColumnStatisticsData._Fields type = ColumnStatisticsData._Fields.valueOf(HBaseUtils.readStr (in));
    ColumnStatisticsData colData = new ColumnStatisticsData();
    switch (type) {
      case BOOLEAN_STATS:
        BooleanColumnStatsData boolData = new BooleanColumnStatsData();
        boolData.setNumTrues(in.readLong());
        boolData.setNumFalses(in.readLong());
        boolData.setNumNulls(in.readLong());
        colData.setBooleanStats(boolData);
        break;

      case LONG_STATS:
        LongColumnStatsData longData = new LongColumnStatsData();
        longData.setLowValue(in.readLong());
        longData.setHighValue(in.readLong());
        longData.setNumNulls(in.readLong());
        longData.setNumDVs(in.readLong());
        colData.setLongStats(longData);
        break;

      case DOUBLE_STATS:
        DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
        doubleData.setLowValue(in.readDouble());
        doubleData.setHighValue(in.readDouble());
        doubleData.setNumNulls(in.readLong());
        doubleData.setNumDVs(in.readLong());
        colData.setDoubleStats(doubleData);
        break;

      case STRING_STATS:
        StringColumnStatsData stringData = new StringColumnStatsData();
        stringData.setMaxColLen(in.readLong());
        stringData.setAvgColLen(in.readDouble());
        stringData.setNumNulls(in.readLong());
        stringData.setNumDVs(in.readLong());
        colData.setStringStats(stringData);
        break;

      case BINARY_STATS:
        BinaryColumnStatsData binaryData = new BinaryColumnStatsData();
        binaryData.setMaxColLen(in.readLong());
        binaryData.setAvgColLen(in.readDouble());
        binaryData.setNumNulls(in.readLong());
        colData.setBinaryStats(binaryData);
        break;

      case DECIMAL_STATS:
        DecimalColumnStatsData decimalData = new DecimalColumnStatsData();
        decimalData.setHighValue(readDecimal(in));
        decimalData.setLowValue(readDecimal(in));
        decimalData.setNumNulls(in.readLong());
        decimalData.setNumDVs(in.readLong());
        colData.setDecimalStats(decimalData);
        break;

      default:
        throw new RuntimeException("Woh, bad.  Unknown stats type!");
    }
    obj.setStatsData(colData);
    return obj;
  }
}
