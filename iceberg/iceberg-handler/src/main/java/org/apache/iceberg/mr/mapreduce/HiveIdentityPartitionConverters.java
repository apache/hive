/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.mapreduce;

import java.math.BigDecimal;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

public class HiveIdentityPartitionConverters {

  private HiveIdentityPartitionConverters() {
  }

  public static Object convertConstant(Type type, Object value) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case STRING:
        return value.toString();
      case TIME:
        return DateTimeUtil.timeFromMicros((Long) value);
      case DATE:
        return Date.ofEpochDay((Integer) value);
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return DateTimeUtil.timestamptzFromMicros((Long) value).toOffsetTime();
        } else {
          return new Timestamp(DateTimeUtil.timestampFromMicros((Long) value));
        }
      case DECIMAL:
        if (value.getClass().isAssignableFrom(BigDecimal.class)) {
          return HiveDecimal.create((BigDecimal) value);
        }
        return value;
      case FIXED:
        if (value instanceof GenericData.Fixed) {
          return ((GenericData.Fixed) value).bytes();
        }
        return value;
      default:
    }
    return value;
  }

}
