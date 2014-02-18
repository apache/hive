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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.conf;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * validate value for a ConfVar, return non-null string for fail message
 */
public interface Validator {

  String validate(String value);

  static class StringSet implements Validator {

    private final Set<String> expected = new LinkedHashSet<String>();

    public StringSet(String... values) {
      for (String value : values) {
        expected.add(value.toLowerCase());
      }
    }

    @Override
    public String validate(String value) {
      if (value == null || !expected.contains(value.toLowerCase())) {
        return "Invalid value.. expects one of " + expected;
      }
      return null;
    }
  }

  static enum RANGE_TYPE {
    INT {
      @Override
      protected boolean inRange(String value, Object lower, Object upper) {
        int ivalue = Integer.parseInt(value);
        return (Integer)lower <= ivalue && ivalue <= (Integer)upper;
      }
    },
    LONG {
      @Override
      protected boolean inRange(String value, Object lower, Object upper) {
        long lvalue = Long.parseLong(value);
        return (Long)lower <= lvalue && lvalue <= (Long)upper;
      }
    },
    FLOAT {
      @Override
      protected boolean inRange(String value, Object lower, Object upper) {
        float fvalue = Float.parseFloat(value);
        return (Float)lower <= fvalue && fvalue <= (Float)upper;
      }
    };

    public static RANGE_TYPE valueOf(Object lower, Object upper) {
      if (lower instanceof Integer && upper instanceof Integer) {
        assert (Integer)lower < (Integer)upper;
        return INT;
      } else if (lower instanceof Long && upper instanceof Long) {
        assert (Long)lower < (Long)upper;
        return LONG;
      } else if (lower instanceof Float && upper instanceof Float) {
        assert (Float)lower < (Float)upper;
        return FLOAT;
      }
      throw new IllegalArgumentException("invalid range from " + lower + " to " + upper);
    }

    protected abstract boolean inRange(String value, Object lower, Object upper);
  }

  static class RangeValidator implements Validator {

    private final RANGE_TYPE type;
    private final Object lower, upper;

    public RangeValidator(Object lower, Object upper) {
      this.lower = lower;
      this.upper = upper;
      this.type = RANGE_TYPE.valueOf(lower, upper);
    }

    @Override
    public String validate(String value) {
      try {
        if (value == null) {
          return "Value cannot be null";
        }
        if (!type.inRange(value.trim(), lower, upper)) {
          return "Invalid value  " + value + ", which should be in between " + lower + " and " + upper;
        }
      } catch (Exception e) {
        return e.toString();
      }
      return null;
    }
  }

  static class PatternSet implements Validator {

    private final List<Pattern> expected = new ArrayList<Pattern>();

    public PatternSet(String... values) {
      for (String value : values) {
        expected.add(Pattern.compile(value));
      }
    }

    @Override
    public String validate(String value) {
      if (value == null) {
        return "Invalid value.. expects one of patterns " + expected;
      }
      for (Pattern pattern : expected) {
        if (pattern.matcher(value).matches()) {
          return null;
        }
      }
      return "Invalid value.. expects one of patterns " + expected;
    }
  }

  static class RatioValidator implements Validator {

    @Override
    public String validate(String value) {
      try {
        float fvalue = Float.valueOf(value);
        if (fvalue <= 0 || fvalue >= 1) {
          return "Invalid ratio " + value + ", which should be in between 0 to 1";
        }
      } catch (NumberFormatException e) {
        return e.toString();
      }
      return null;
    }
  }
}
