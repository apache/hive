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
package org.apache.hadoop.hive.ql.udf.esri.shims;

import java.lang.reflect.Method;
import java.util.TimeZone;

public class HiveShims {

  /**
   * This class is supplied for compatibility between Hive versions.
   * At 0.10 the serde constants were moved to another package.  Also,
   * at 0.11 the previous class will be re-added for backwards
   * compatibility, but deprecated.
   *
   */
  public static class serdeConstants {
    public static final String LIST_COLUMNS;
    public static final String LIST_COLUMN_TYPES;

    static {
      Class<?> clazz = null;

      try {
        // Hive 0.10 and above constants
        clazz = Class.forName("org.apache.hadoop.hive.serde.serdeConstants");
      } catch (ClassNotFoundException e) {
        try {
          // Hive 0.9 and below constants
          clazz = Class.forName("org.apache.hadoop.hive.serde.Constants");
        } catch (ClassNotFoundException e1) {
          // not much we can do here
        }
      }

      LIST_COLUMNS = getAsStringOrNull(clazz, "LIST_COLUMNS");
      LIST_COLUMN_TYPES = getAsStringOrNull(clazz, "LIST_COLUMN_TYPES");
    }

    static String getAsStringOrNull(Class<?> clazz, String constant) {
      try {
        return (String) clazz.getField(constant).get(null);
      } catch (Exception e) {
        return null;
      }
    }
  }

  /**
   * Classes o.a.h.h.common.type Date and Timestamp were introduced in Hive-3.1 version.
   */
  public static Long getPrimitiveEpoch(Object prim, TimeZone tz) {
    if (prim instanceof java.sql.Timestamp) {
      return ((java.sql.Timestamp) prim).getTime();
    } else if (prim instanceof java.util.Date) {
      return ((java.util.Date) prim).getTime();
    } else {
      try {
        Class<?> dtClazz = Class.forName("org.apache.hadoop.hive.common.type.Date");
        if (prim.getClass() == dtClazz) {
          Method dtGetImpl = dtClazz.getMethod("toEpochMilli");
          return (java.lang.Long) (dtGetImpl.invoke(prim));
        } else {
          Class<?> ttClazz = Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
          if (prim.getClass() == ttClazz) {
            Method ttGetImpl = ttClazz.getMethod("toEpochMilli");
            return (java.lang.Long) (ttGetImpl.invoke(prim));
          } else {
            return null;
          }
        }
      } catch (Exception exc) {
        return null;
      }
    }
  }

  /**
   * Type DATE was introduced in Hive-0.12 - class DateWritable in API.
   * Class DateWritableV2 is used instead as of Hive-3.1 version.
   */
  public static void setDateWritable(Object dwHive, long epoch, TimeZone tz) {
    try {                                // Hive 3.1 and above
      Class<?> dtClazz = Class.forName("org.apache.hadoop.hive.common.type.Date");
      Class<?> dwClazz = Class.forName("org.apache.hadoop.hive.serde2.io.DateWritableV2");
      Method dtSetImpl = dtClazz.getMethod("setTimeInMillis", long.class);
      Method dwSetImpl = dwClazz.getMethod("set", dtClazz);
      Object dtObj = dtClazz.getConstructor().newInstance();
      dtSetImpl.invoke(dtObj, epoch);
      dwSetImpl.invoke(dwHive, dtObj);
    } catch (Exception e1) {
      try {                            // Hive 0.12 and above
        Class<?> dwClazz = Class.forName("org.apache.hadoop.hive.serde2.io.DateWritable");
        Method dwSetImpl = dwClazz.getMethod("set", java.sql.Date.class);
        dwSetImpl.invoke(dwHive, new java.sql.Date(epoch));
      } catch (Exception e2) {              // Hive 0.11 and below
        // column type DATE not supported
        throw new UnsupportedOperationException("DATE type");
      }
    }
  }  // setDateWritable

  /**
   * Type DATE was introduced in Hive-0.12 - class DateWritable in API.
   * Class DateWritableV2 is used instead as of Hive-3.1 version.
   */
  public static void setDateWritable(Object dwHive, java.sql.Date jsd) {
    try {                                // Hive 3.1 and above
      Class<?> dtClazz = Class.forName("org.apache.hadoop.hive.common.type.Date");
      Class<?> dwClazz = Class.forName("org.apache.hadoop.hive.serde2.io.DateWritableV2");
      Method dtSetImpl = dtClazz.getMethod("setTimeInMillis", long.class);
      Method dwSetImpl = dwClazz.getMethod("set", dtClazz);
      Object dtObj = dtClazz.getConstructor().newInstance();
      dtSetImpl.invoke(dtObj, jsd.getTime());
      dwSetImpl.invoke(dwHive, dtObj);
    } catch (Exception e1) {
      try {                            // Hive 0.12 and above
        Class<?> dwClazz = Class.forName("org.apache.hadoop.hive.serde2.io.DateWritable");
        Method dwSetImpl = dwClazz.getMethod("set", java.sql.Date.class);
        dwSetImpl.invoke(dwHive, jsd);
      } catch (Exception e2) {              // Hive 0.11 and below
        // column type DATE not supported
        throw new UnsupportedOperationException("DATE type");
      }
    }
  }  // setDateWritable

  /**
   * Type TIMESTAMP was introduced in Hive-0.12 - class TimestampWritable in API.
   * Class TimestampWritableV2 is used instead as of Hive-3.1 version.
   */
  public static void setTimeWritable(Object twHive, long epoch) {
    try {                                // Hive 3.1 and above
      Class<?> ttClazz = Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
      Class<?> twClazz = Class.forName("org.apache.hadoop.hive.serde2.io.TimestampWritableV2");
      Method ttSetImpl = ttClazz.getMethod("setTimeInMillis", long.class);
      Method twSetImpl = twClazz.getMethod("set", ttClazz);
      Object ttObj = ttClazz.getConstructor().newInstance();
      ttSetImpl.invoke(ttObj, epoch);
      twSetImpl.invoke(twHive, ttObj);
    } catch (Exception e1) {
      try {                            // Hive 0.12 and above
        Class<?> twClazz = Class.forName("org.apache.hadoop.hive.serde2.io.TimestampWritable");
        Method twSetImpl = twClazz.getMethod("set", java.sql.Timestamp.class);
        twSetImpl.invoke(twHive, new java.sql.Timestamp(epoch));
      } catch (Exception e2) {              // Hive 0.11 and below
        // column type TIMESTAMP not supported
        throw new UnsupportedOperationException("TIMESTAMP type");
      }
    }
  }  // setTimeWritable

  /**
   * Type TIMESTAMP was introduced in Hive-0.12 - class TimestampWritable in API.
   * Class TimestampWritableV2 is used instead as of Hive-3.1 version.
   */
  public static void setTimeWritable(Object twHive, java.sql.Timestamp jst) {
    long epoch = jst.getTime();
    try {                                // Hive 3.1 and above
      Class<?> ttClazz = Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
      Class<?> twClazz = Class.forName("org.apache.hadoop.hive.serde2.io.TimestampWritableV2");
      Method ttSetImpl = ttClazz.getMethod("setTimeInMillis", long.class);
      Method twSetImpl = twClazz.getMethod("set", ttClazz);
      Object ttObj = ttClazz.getConstructor().newInstance();
      ttSetImpl.invoke(ttObj, epoch);
      twSetImpl.invoke(twHive, ttObj);
    } catch (Exception e1) {
      try {                            // Hive 0.12 and above
        Class<?> twClazz = Class.forName("org.apache.hadoop.hive.serde2.io.TimestampWritable");
        Method twSetImpl = twClazz.getMethod("set", java.sql.Timestamp.class);
        twSetImpl.invoke(twHive, new java.sql.Timestamp(epoch));
      } catch (Exception e2) {              // Hive 0.11 and below
        // column type TIMESTAMP not supported
        throw new UnsupportedOperationException("TIMESTAMP type");
      }
    }
  }  // setTimeWritable
}
