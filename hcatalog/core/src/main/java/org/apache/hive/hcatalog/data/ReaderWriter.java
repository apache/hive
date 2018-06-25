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
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;

/*
 * when adding support for new types, we should try to use classes of Hive value system to keep 
 * things more readable (though functionally it should not make a difference). 
 */
public abstract class ReaderWriter {

  private static final String UTF8 = "UTF-8";

  public static Object readDatum(DataInput in) throws IOException {

    byte type = in.readByte();
    switch (type) {

    case DataType.STRING:
      byte[] buffer = new byte[in.readInt()];
      in.readFully(buffer);
      return new String(buffer, UTF8);

    case DataType.INTEGER:
      VIntWritable vint = new VIntWritable();
      vint.readFields(in);
      return vint.get();

    case DataType.LONG:
      VLongWritable vlong = new VLongWritable();
      vlong.readFields(in);
      return vlong.get();

    case DataType.FLOAT:
      return in.readFloat();

    case DataType.DOUBLE:
      return in.readDouble();

    case DataType.BOOLEAN:
      return in.readBoolean();

    case DataType.BYTE:
      return in.readByte();

    case DataType.SHORT:
      return in.readShort();

    case DataType.NULL:
      return null;

    case DataType.BINARY:
      int len = in.readInt();
      byte[] ba = new byte[len];
      in.readFully(ba);
      return ba;

    case DataType.MAP:
      int size = in.readInt();
      Map<Object, Object> m = new LinkedHashMap<Object, Object>(size);
      for (int i = 0; i < size; i++) {
        m.put(readDatum(in), readDatum(in));
      }
      return m;

    case DataType.LIST:
      int sz = in.readInt();
      List<Object> list = new ArrayList<Object>(sz);
      for (int i = 0; i < sz; i++) {
        list.add(readDatum(in));
      }
      return list;
    case DataType.CHAR:
      HiveCharWritable hcw = new HiveCharWritable();
      hcw.readFields(in);
      return hcw.getHiveChar();
    case DataType.VARCHAR:
      HiveVarcharWritable hvw = new HiveVarcharWritable();
      hvw.readFields(in);
      return hvw.getHiveVarchar();
    case DataType.DECIMAL:
      HiveDecimalWritable hdw = new HiveDecimalWritable();
      hdw.readFields(in);
      return hdw.getHiveDecimal();
    case DataType.DATE:
      DateWritable dw = new DateWritable();
      dw.readFields(in);
      return dw.get();
    case DataType.TIMESTAMP:
      TimestampWritable tw = new TimestampWritable();
      tw.readFields(in);
      return tw.getTimestamp();
    default:
      throw new IOException("Unexpected data type " + type +
        " found in stream.");
    }
  }

  public static void writeDatum(DataOutput out, Object val) throws IOException {
    // write the data type
    byte type = DataType.findType(val);
    out.write(type);
    switch (type) {
    case DataType.LIST:
      List<?> list = (List<?>) val;
      int sz = list.size();
      out.writeInt(sz);
      for (int i = 0; i < sz; i++) {
        writeDatum(out, list.get(i));
      }
      return;

    case DataType.MAP:
      Map<?, ?> m = (Map<?, ?>) val;
      out.writeInt(m.size());
      Iterator<?> i =
        m.entrySet().iterator();
      while (i.hasNext()) {
        Entry<?, ?> entry = (Entry<?, ?>) i.next();
        writeDatum(out, entry.getKey());
        writeDatum(out, entry.getValue());
      }
      return;

    case DataType.INTEGER:
      new VIntWritable((Integer) val).write(out);
      return;

    case DataType.LONG:
      new VLongWritable((Long) val).write(out);
      return;

    case DataType.FLOAT:
      out.writeFloat((Float) val);
      return;

    case DataType.DOUBLE:
      out.writeDouble((Double) val);
      return;

    case DataType.BOOLEAN:
      out.writeBoolean((Boolean) val);
      return;

    case DataType.BYTE:
      out.writeByte((Byte) val);
      return;

    case DataType.SHORT:
      out.writeShort((Short) val);
      return;

    case DataType.STRING:
      String s = (String) val;
      byte[] utfBytes = s.getBytes(ReaderWriter.UTF8);
      out.writeInt(utfBytes.length);
      out.write(utfBytes);
      return;

    case DataType.BINARY:
      byte[] ba = (byte[]) val;
      out.writeInt(ba.length);
      out.write(ba);
      return;

    case DataType.NULL:
      //for NULL we just write out the type
      return;
    case DataType.CHAR:
      new HiveCharWritable((HiveChar)val).write(out);
      return;
    case DataType.VARCHAR:
      new HiveVarcharWritable((HiveVarchar)val).write(out);
      return;
    case DataType.DECIMAL:
      new HiveDecimalWritable((HiveDecimal)val).write(out);
      return;
    case DataType.DATE:
      new DateWritable((Date)val).write(out);
      return;
    case DataType.TIMESTAMP:
      new TimestampWritable((java.sql.Timestamp)val).write(out);
      return;
    default:
      throw new IOException("Unexpected data type " + type +
        " found in stream.");
    }
  }
}
