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

package org.apache.hadoop.hive.conf;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * validate value for a ConfVar, return non-null string for fail message
 */
public interface Validator {

  String validate(String value);

  String toDescription();

  class StringSet implements Validator {

    private final boolean caseSensitive;
    private final Set<String> expected = new LinkedHashSet<String>();

    public StringSet(String... values) {
      this(false, values);
    }

    public StringSet(boolean caseSensitive, String... values) {
      this.caseSensitive = caseSensitive;
      for (String value : values) {
        expected.add(caseSensitive ? value : value.toLowerCase());
      }
    }

    public Set<String> getExpected() {
      return new HashSet<String>(expected);
    }

    @Override
    public String validate(String value) {
      if (value == null || !expected.contains(caseSensitive ? value : value.toLowerCase())) {
        return "Invalid value.. expects one of " + expected;
      }
      return null;
    }

    @Override
    public String toDescription() {
      return "Expects one of " + expected;
    }
  }

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

  class RangeValidator implements Validator {

    private final TYPE type;
    private final Object lower, upper;

    public RangeValidator(Object lower, Object upper) {
      this.lower = lower;
      this.upper = upper;
      this.type = TYPE.valueOf(lower, upper);
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

    @Override
    public String toDescription() {
      if (lower == null && upper == null) {
        return null;
      }
      if (lower != null && upper != null) {
        return "Expects value between " + lower + " and " + upper;
      }
      if (lower != null) {
        return "Expects value bigger than " + lower;
      }
      return "Expects value smaller than " + upper;
    }
  }

  class PatternSet implements Validator {

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

    @Override
    public String toDescription() {
      return "Expects one of the pattern in " + expected;
    }
  }

  class RatioValidator implements Validator {

    @Override
    public String validate(String value) {
      try {
        float fvalue = Float.parseFloat(value);
        if (fvalue < 0 || fvalue > 1) {
          return "Invalid ratio " + value + ", which should be in between 0 to 1";
        }
      } catch (NumberFormatException e) {
        return e.toString();
      }
      return null;
    }

    @Override
    public String toDescription() {
      return "Expects value between 0.0f and 1.0f";
    }
  }

  class TimeValidator implements Validator {

    private final TimeUnit timeUnit;

    private final Long min;
    private final boolean minInclusive;

    private final Long max;
    private final boolean maxInclusive;

    public TimeValidator(TimeUnit timeUnit) {
      this(timeUnit, null, false, null, false);
    }

    public TimeValidator(TimeUnit timeUnit,
        Long min, boolean minInclusive, Long max, boolean maxInclusive) {
      this.timeUnit = timeUnit;
      this.min = min;
      this.minInclusive = minInclusive;
      this.max = max;
      this.maxInclusive = maxInclusive;
    }

    public TimeUnit getTimeUnit() {
      return timeUnit;
    }

    @Override
    public String validate(String value) {
      try {
        long time = HiveConf.toTime(value, timeUnit, timeUnit);
        if (min != null && (minInclusive ? time < min : time <= min)) {
          return value + " is smaller than " + timeString(min);
        }
        if (max != null && (maxInclusive ? time > max : time >= max)) {
          return value + " is bigger than " + timeString(max);
        }
      } catch (Exception e) {
        return e.toString();
      }
      return null;
    }

    public String toDescription() {
      String description =
          "Expects a time value with unit " +
          "(d/day, h/hour, m/min, s/sec, ms/msec, us/usec, ns/nsec)" +
          ", which is " + HiveConf.stringFor(timeUnit) + " if not specified";
      if (min != null && max != null) {
        description += ".\nThe time should be in between " +
            timeString(min) + (minInclusive ? " (inclusive)" : " (exclusive)") + " and " +
            timeString(max) + (maxInclusive ? " (inclusive)" : " (exclusive)");
      } else if (min != null) {
        description += ".\nThe time should be bigger than " +
            (minInclusive ? "or equal to " : "") + timeString(min);
      } else if (max != null) {
        description += ".\nThe time should be smaller than " +
            (maxInclusive ? "or equal to " : "") + timeString(max);
      }
      return description;
    }

    private String timeString(long time) {
      return time + " " + HiveConf.stringFor(timeUnit);
    }
  }


  class SizeValidator implements Validator {

    private final Long min;
    private final boolean minInclusive;

    private final Long max;
    private final boolean maxInclusive;

    public SizeValidator() {
      this(null, false, null, false);
    }

    public SizeValidator(Long min, boolean minInclusive, Long max, boolean maxInclusive) {
      this.min = min;
      this.minInclusive = minInclusive;
      this.max = max;
      this.maxInclusive = maxInclusive;
    }

    @Override
    public String validate(String value) {
      try {
        long size = HiveConf.toSizeBytes(value);
        if (min != null && (minInclusive ? size < min : size <= min)) {
          return value + " is smaller than " + sizeString(min);
        }
        if (max != null && (maxInclusive ? size > max : size >= max)) {
          return value + " is bigger than " + sizeString(max);
        }
      } catch (Exception e) {
        return e.toString();
      }
      return null;
    }

    public String toDescription() {
      String description =
          "Expects a byte size value with unit (blank for bytes, kb, mb, gb, tb, pb)";
      if (min != null && max != null) {
        description += ".\nThe size should be in between " +
            sizeString(min) + (minInclusive ? " (inclusive)" : " (exclusive)") + " and " +
            sizeString(max) + (maxInclusive ? " (inclusive)" : " (exclusive)");
      } else if (min != null) {
        description += ".\nThe time should be bigger than " +
            (minInclusive ? "or equal to " : "") + sizeString(min);
      } else if (max != null) {
        description += ".\nThe size should be smaller than " +
            (maxInclusive ? "or equal to " : "") + sizeString(max);
      }
      return description;
    }

    private String sizeString(long size) {
      final String[] units = { " bytes", "Kb", "Mb", "Gb", "Tb" };
      long current = 1;
      for (int i = 0; i < units.length && current > 0; ++i) {
        long next = current << 10;
        if ((size & (next - 1)) != 0) return (long)(size / current) + units[i];
        current = next;
      }
      return current > 0 ? ((long)(size / current) + "Pb") : (size + units[0]);
    }
  }

  public class WritableDirectoryValidator implements Validator {

    @Override
    public String validate(String value) {
      final Path path = FileSystems.getDefault().getPath(value);
      if (path == null && value != null) {
        return String.format("Path '%s' provided could not be located.", value);
      }
      final boolean isDir = Files.isDirectory(path);
      final boolean isWritable = Files.isWritable(path);
      if (!isDir) {
        return String.format("Path '%s' provided is not a directory.", value);
      }
      if (!isWritable) {
        return String.format("Path '%s' provided is not writable.", value);
      }
      return null;
    }

    @Override
    public String toDescription() {
      return "Expects a writable directory on the local filesystem";
    }
  }

}
