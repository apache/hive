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

public class SizeValidator implements Validator {
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
  public void validate(String value) {
    long size = toSizeBytes(value);
    if (min != null && (minInclusive ? size < min : size <= min)) {
      throw new IllegalArgumentException(
          value + " is smaller than minimum " + min + sizeString(min));
    }
    if (max != null && (maxInclusive ? size > max : size >= max)) {
      throw new IllegalArgumentException(
          value + " is larger than maximum " + max + sizeString(max));
    }
  }

  public String toDescription() {
    String description =
        "Expects a byte size value with unit (blank for bytes, kb, mb, gb, tb, pb)";
    if (min != null && max != null) {
      description += ".\nThe size should be in between " + sizeString(min)
          + (minInclusive ? " (inclusive)" : " (exclusive)") + " and " + sizeString(max)
          + (maxInclusive ? " (inclusive)" : " (exclusive)");
    } else if (min != null) {
      description += ".\nThe time should be bigger than " + (minInclusive ? "or equal to " : "")
          + sizeString(min);
    } else if (max != null) {
      description += ".\nThe size should be smaller than " + (maxInclusive ? "or equal to " : "")
          + sizeString(max);
    }
    return description;
  }

  private String sizeString(long size) {
    final String[] units = { " bytes", "Kb", "Mb", "Gb", "Tb" };
    long current = 1;
    for (int i = 0; i < units.length && current > 0; ++i) {
      long next = current << 10;
      if ((size & (next - 1)) != 0)
        return (long) (size / current) + units[i];
      current = next;
    }
    return current > 0 ? ((long) (size / current) + "Pb") : (size + units[0]);
  }

  public static long toSizeBytes(String value) {
    String[] parsed = parseNumberFollowedByUnit(value.trim());
    return Long.parseLong(parsed[0].trim()) * multiplierFor(parsed[1].trim());
  }

  private static String[] parseNumberFollowedByUnit(String value) {
    char[] chars = value.toCharArray();
    int i = 0;
    for (; i < chars.length && (chars[i] == '-' || Character.isDigit(chars[i])); i++) {
    }
    return new String[] { value.substring(0, i), value.substring(i) };
  }

  private static long multiplierFor(String unit) {
    unit = unit.trim().toLowerCase();
    if (unit.isEmpty() || unit.equals("b") || unit.equals("bytes")) {
      return 1;
    } else if (unit.equals("kb")) {
      return 1024;
    } else if (unit.equals("mb")) {
      return 1024 * 1024;
    } else if (unit.equals("gb")) {
      return 1024 * 1024 * 1024;
    } else if (unit.equals("tb")) {
      return 1024L * 1024 * 1024 * 1024;
    } else if (unit.equals("pb")) {
      return 1024L * 1024 * 1024 * 1024 * 1024;
    }
    throw new IllegalArgumentException("Invalid size unit " + unit);
  }
}
