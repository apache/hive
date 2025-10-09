/**
 * Licensed to the Apache Software Foundation (ASF) under one§
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.queryhistory.schema;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.queryhistory.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class QueryHistorySchemaTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(QueryHistorySchemaTestUtils.class);

  /**
   * Special equality method for datetime fields in a Record. This is because hive's Timestamp
   * after being serialized and de-serialized ends up becoming a LocalDateTime, and a TimeStamp.equals
   * (LocalDateTime) would simply return non-equality, even if the inner LocalDateTime instance points to the
   * same moment in time as the other LocalDateTime instance.
   */
  public static boolean queryHistoryRecordsAreEqual(Record thiz, Record obj){
    if (thiz == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    Record other = (Record) obj;

    for (Schema.Field field : Schema.Field.values()) {
      Object valueThis = thiz.get(field);
      Object valueThat = other.get(field);
      if ((valueThis != null && valueThat == null) || (valueThis == null && valueThat != null)) {
        LOG.info("Record equality check: fields differ in null value: {}", field.getName());
        return false;
      }
      if (valueThis instanceof Timestamp || valueThat instanceof Timestamp){
        boolean timesEquals = compareTimes(valueThis, valueThat);
        if (!timesEquals){
          return false;
        }
        continue; // check next field
      }
      if (valueThis != null && !valueThis.equals(valueThat)) {
        LOG.info("Record equality check: fields are not equal: {}, this: {} (type: {})," +
            " that: {} (type: {})",
            field.getName(),
            valueThis, valueThis.getClass(), valueThat, valueThat.getClass());
        return false;
      }
    }
    return true;
  }

  private static boolean compareTimes(Object valueThis, Object valueThat) {
    Timestamp d1 = valueThis instanceof Timestamp ? (Timestamp) valueThis : new Timestamp((LocalDateTime) valueThis);
    Timestamp d2 = valueThat instanceof Timestamp ? (Timestamp) valueThat : new Timestamp((LocalDateTime) valueThat);

    int diff = d1.compareTo(d2);
    if (diff != 0){
      LOG.info("Timestamps aren't equal: {} <-> {}", d1, d2);
    }
    return diff == 0;
  }
}
