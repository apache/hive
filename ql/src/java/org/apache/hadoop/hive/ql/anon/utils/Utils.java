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

package org.apache.hadoop.hive.ql.anon.utils;

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.anon.ConstCode;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;

public class Utils {

  public static byte[] writableToBytes(Writable w) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    w.write(dos);
    dos.flush();
    dos.close();
    return baos.toByteArray();
  }

  public static void bytesToWritable(byte[] bytes, Writable w) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);
    w.readFields(dis);
    bais.close();
  }

  public static MapWritable convert(final Object o) throws IOException {
    final BytesWritable bw = (BytesWritable) o;
    final MapWritable map = new MapWritable();
    bytesToWritable(bw.getBytes(), map);
    return map;
  }

  public static Writable createWritable(char type) {
    switch (type) {
      case BTREE_LONG_TYPE:
        return new LongWritable();
      case BTREE_INT_TYPE:
        return new IntWritable();
      case BTREE_TEXT_TYPE:
        return new Text();
      default:
        throw new RuntimeException("Unsupported type: " + type);
    }
  }

  public static List<WritableComparable> parsePidList(String pidCsv, String pt) {
    List<WritableComparable> keys = new ArrayList<>();
    PidType pidType = PidType.valueOf(pt.toUpperCase());

    for (String s : pidCsv.split(",")) {
      final String t = s.trim();
      if (t.isEmpty()) {
        continue;
      }
      keys.add(getKey(t, pidType));
    }
    return keys;
  }

  private static WritableComparable getKey(String s, PidType pidType) {
    switch (pidType) {
      case INT: {
        int pid = Integer.parseInt(s);
        return new IntWritable(pid);
      }
      case BIGINT:
      case LONG: {
        long pid = Long.parseLong(s);
        return new LongWritable(pid);
      }
      case STRING: {
        return new Text(s);
      }
      default: {
        throw new IllegalArgumentException("Unknown pid type: " + pidType);
      }
    }
  }

  private enum PidType {
    INT("int"),
    BIGINT("bigint"),
    LONG("long"),
    STRING("string");

    private final String type;

    PidType(String type) {
      this.type = type;
    }

    public String getType() {
      return type;
    }
  }

  public static String getAddrType(Index index) throws SemanticException {
    return getType(index.getPointerType());
  }

  public static String getType(String type) {
    char ret;
    switch (type) {
      case "int":
        ret = BTREE_INT_TYPE;
        break;
      case "bigint":
      case "long":
        ret = BTREE_LONG_TYPE;
        break;
      case "string":
        ret = BTREE_TEXT_TYPE;
        break;
      default:
        throw new RuntimeException("unknown type: " + type);
    }

    return "" + ret;
  }

  public static String headerType(final byte headerByte, final String fallback) {
    return headerByte > 0 ? String.valueOf((char) headerByte) : fallback;
  }

  public static String headerTypes(final byte[] headerBytes, final String fallback) {
    final StringBuilder sb = new StringBuilder(headerBytes.length);
    for (final byte b : headerBytes) {
      if (b <= 0) {
        return fallback;
      }
      sb.append((char) b);
    }
    return sb.toString();
  }

  public static int convertNumberWritable(WritableComparable wc) {
    if (wc instanceof IntWritable) {
      return ((IntWritable) wc).get();
    } else if (wc instanceof LongWritable) {
      return (int) ((LongWritable) wc).get();
    } else {
      throw new RuntimeException("Unsupported type: " + wc);
    }
  }

  public static ConstCode getConstCode(final ColumnInternalFormat internalFormat) {
    switch (internalFormat) {
      case JSON:
        return ConstCode.j;
      case MSGPACK:
        return ConstCode.m;
      case XML:
        return ConstCode.x;
      case PROTOBUF:
        return ConstCode.p;
      case AVRO:
        return ConstCode.a;
      default:
        throw new IllegalArgumentException("bad format");
    }
  }
}
