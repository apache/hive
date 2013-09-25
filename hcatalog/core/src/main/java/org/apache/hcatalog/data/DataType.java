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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.data.DataType} instead
 */
public abstract class DataType {

  public static final byte NULL = 1;
  public static final byte BOOLEAN = 5;
  public static final byte BYTE = 6;
  public static final byte INTEGER = 10;
  public static final byte SHORT = 11;
  public static final byte LONG = 15;
  public static final byte FLOAT = 20;
  public static final byte DOUBLE = 25;
  public static final byte STRING = 55;
  public static final byte BINARY = 60;

  public static final byte MAP = 100;
  public static final byte STRUCT = 110;
  public static final byte LIST = 120;
  public static final byte ERROR = -1;

  /**
   * Determine the datatype of an object.
   * @param o Object to test.
   * @return byte code of the type, or ERROR if we don't know.
   */
  public static byte findType(Object o) {
    if (o == null) {
      return NULL;
    }

    Class<?> clazz = o.getClass();

    // Try to put the most common first
    if (clazz == String.class) {
      return STRING;
    } else if (clazz == Integer.class) {
      return INTEGER;
    } else if (clazz == Long.class) {
      return LONG;
    } else if (clazz == Float.class) {
      return FLOAT;
    } else if (clazz == Double.class) {
      return DOUBLE;
    } else if (clazz == Boolean.class) {
      return BOOLEAN;
    } else if (clazz == Byte.class) {
      return BYTE;
    } else if (clazz == Short.class) {
      return SHORT;
    } else if (o instanceof List<?>) {
      return LIST;
    } else if (o instanceof Map<?, ?>) {
      return MAP;
    } else if (o instanceof byte[]) {
      return BINARY;
    } else {
      return ERROR;
    }
  }

  public static int compare(Object o1, Object o2) {

    return compare(o1, o2, findType(o1), findType(o2));
  }

  public static int compare(Object o1, Object o2, byte dt1, byte dt2) {
    if (dt1 == dt2) {
      switch (dt1) {
      case NULL:
        return 0;

      case BOOLEAN:
        return ((Boolean) o1).compareTo((Boolean) o2);

      case BYTE:
        return ((Byte) o1).compareTo((Byte) o2);

      case INTEGER:
        return ((Integer) o1).compareTo((Integer) o2);

      case LONG:
        return ((Long) o1).compareTo((Long) o2);

      case FLOAT:
        return ((Float) o1).compareTo((Float) o2);

      case DOUBLE:
        return ((Double) o1).compareTo((Double) o2);

      case STRING:
        return ((String) o1).compareTo((String) o2);

      case SHORT:
        return ((Short) o1).compareTo((Short) o2);

      case BINARY:
        return compareByteArray((byte[]) o1, (byte[]) o2);

      case LIST:
        List<?> l1 = (List<?>) o1;
        List<?> l2 = (List<?>) o2;
        int len = l1.size();
        if (len != l2.size()) {
          return len - l2.size();
        } else {
          for (int i = 0; i < len; i++) {
            int cmpVal = compare(l1.get(i), l2.get(i));
            if (cmpVal != 0) {
              return cmpVal;
            }
          }
          return 0;
        }

      case MAP: {
        Map<?, ?> m1 = (Map<?, ?>) o1;
        Map<?, ?> m2 = (Map<?, ?>) o2;
        int sz1 = m1.size();
        int sz2 = m2.size();
        if (sz1 < sz2) {
          return -1;
        } else if (sz1 > sz2) {
          return 1;
        } else {
          // This is bad, but we have to sort the keys of the maps in order
          // to be commutative.
          TreeMap<Object, Object> tm1 = new TreeMap<Object, Object>(m1);
          TreeMap<Object, Object> tm2 = new TreeMap<Object, Object>(m2);
          Iterator<Entry<Object, Object>> i1 = tm1.entrySet().iterator();
          Iterator<Entry<Object, Object>> i2 = tm2.entrySet().iterator();
          while (i1.hasNext()) {
            Map.Entry<Object, Object> entry1 = i1.next();
            Map.Entry<Object, Object> entry2 = i2.next();
            int c = compare(entry1.getValue(), entry2.getValue());
            if (c != 0) {
              return c;
            } else {
              c = compare(entry1.getValue(), entry2.getValue());
              if (c != 0) {
                return c;
              }
            }
          }
          return 0;
        }
      }

      default:
        throw new RuntimeException("Unkown type " + dt1 +
          " in compare");
      }
    } else {
      return dt1 < dt2 ? -1 : 1;
    }
  }

  private static int compareByteArray(byte[] o1, byte[] o2) {

    for (int i = 0; i < o1.length; i++) {
      if (i == o2.length) {
        return 1;
      }
      if (o1[i] == o2[i]) {
        continue;
      }
      if (o1[i] > o1[i]) {
        return 1;
      } else {
        return -1;
      }
    }

    //bytes in o1 are same as o2
    //in case o2 was longer
    if (o2.length > o1.length) {
      return -1;
    }
    return 0; //equals
  }

}
