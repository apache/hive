/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.conf;

/**
 * validate value for a ConfVar, return non-null string for fail message
 */
public interface Validator {

  /**
   * Validate if the given value is acceptable.
   * @param value value to test
   * @throws IllegalArgumentException if the value is invalid
   */
  void validate(String value) throws IllegalArgumentException;

  enum TYPE {
    INT {
      @Override
      protected boolean inRange(String value, Object lower, Object upper) {
        int ivalue = Integer.parseInt(value);
        if (lower != null && ivalue < (Integer)lower) {
          return false;
        }
        if (upper != null && ivalue > (Integer)upper) {
          return false;
        }
        return true;
      }
    },
    LONG {
      @Override
      protected boolean inRange(String value, Object lower, Object upper) {
        long lvalue = Long.parseLong(value);
        if (lower != null && lvalue < (Long)lower) {
          return false;
        }
        if (upper != null && lvalue > (Long)upper) {
          return false;
        }
        return true;
      }
    },
    FLOAT {
      @Override
      protected boolean inRange(String value, Object lower, Object upper) {
        float fvalue = Float.parseFloat(value);
        if (lower != null && fvalue < (Float)lower) {
          return false;
        }
        if (upper != null && fvalue > (Float)upper) {
          return false;
        }
        return true;
      }
    };

    public static TYPE valueOf(Object lower, Object upper) {
      if (lower instanceof Integer || upper instanceof Integer) {
        return INT;
      } else if (lower instanceof Long || upper instanceof Long) {
        return LONG;
      } else if (lower instanceof Float || upper instanceof Float) {
        return FLOAT;
      }
      throw new IllegalArgumentException("invalid range from " + lower + " to " + upper);
    }

    protected abstract boolean inRange(String value, Object lower, Object upper);
  }

}
