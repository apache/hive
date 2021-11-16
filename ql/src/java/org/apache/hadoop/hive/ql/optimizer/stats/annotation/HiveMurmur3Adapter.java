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
  
package org.apache.hadoop.hive.ql.optimizer.stats.annotation;

import java.nio.ByteBuffer;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hive.common.util.Murmur3;

/**
* This class could be used to map Hive values type to Murmur3 hash values.
*/
public class HiveMurmur3Adapter {

  private PrimitiveCategory type;
  private PrimitiveObjectInspector inputOI;

  public HiveMurmur3Adapter(PrimitiveObjectInspector oi) throws HiveException {
    this.inputOI = oi;
    type = oi.getTypeInfo().getPrimitiveCategory();
  }

  private final ByteBuffer LONG_BUFFER = ByteBuffer.allocate(Long.BYTES);

  public long murmur3(Object objVal) throws HiveException {
    Object p = objVal;
    switch (type) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case TIMESTAMP: {
        long val = PrimitiveObjectInspectorUtils.getLong(objVal, inputOI);
        LONG_BUFFER.putLong(0, val);
        return Murmur3.hash64(LONG_BUFFER.array());
      }
      case FLOAT:
      case DOUBLE: {
        double val = PrimitiveObjectInspectorUtils.getDouble(objVal, inputOI);
        LONG_BUFFER.putDouble(0, val);
        return Murmur3.hash64(LONG_BUFFER.array());
      }
      case STRING:
      case CHAR:
      case VARCHAR: {
        String val = PrimitiveObjectInspectorUtils.getString(objVal, inputOI);
        return Murmur3.hash64(val.getBytes());
      }
      case DECIMAL: {
          HiveDecimal decimal = PrimitiveObjectInspectorUtils.getHiveDecimal(p, inputOI);
          LONG_BUFFER.putDouble(0, decimal.doubleValue());
          return Murmur3.hash64(LONG_BUFFER.array());
        }
      case DATE:
          DateWritable v = new DateWritable((DateWritable) inputOI.getPrimitiveWritableObject(p));
          int days = v.getDays();
          LONG_BUFFER.putLong(0, days);
          return Murmur3.hash64(LONG_BUFFER.array());
      case BOOLEAN:
      case BINARY:
      default:
        throw new HiveException("type: " + type + " is not supported");
    }
  }
}