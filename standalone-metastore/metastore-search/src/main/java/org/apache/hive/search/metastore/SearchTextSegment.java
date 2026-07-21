/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.metastore;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

/** Structured split of table search text: optional head (table comment) then column batches. */
public final class SearchTextSegment {
  public static final String SEGMENT_PREFIX = "search_text_";

  private SearchTextSegment() {}

  public static String segmentField(int index) {
    if (index < 0) {
      throw new IllegalArgumentException("segment index must be >= 0");
    }
    return SEGMENT_PREFIX + index;
  }

  public static boolean isSegmentField(String fieldName) {
    if (fieldName == null || !fieldName.startsWith(SEGMENT_PREFIX)) {
      return false;
    }
    String suffix = fieldName.substring(SEGMENT_PREFIX.length());
    return !suffix.isEmpty() && suffix.chars().allMatch(Character::isDigit);
  }

  public static int segmentIndex(String fieldName) {
    if (!isSegmentField(fieldName)) {
      throw new IllegalArgumentException("not a search text segment field: " + fieldName);
    }
    return Integer.parseInt(fieldName.substring(SEGMENT_PREFIX.length()));
  }

  /**
   * @param maxSegments max {@code search_text_N} fields for this table
   * @param maxCharsPerSegment soft char limit per segment
   */
  public static List<String> build(Table table, int maxSegments, int maxCharsPerSegment) {
    if (maxSegments < 1) {
      throw new IllegalArgumentException("maxSegments must be >= 1");
    }
    List<String> result = new ArrayList<>(maxSegments);
    String comment = tableComment(table);
    if (StringUtils.isNotEmpty(comment)) {
      String head = buildHead(table.getTableName(), comment);
      if (head.length() > maxCharsPerSegment) {
        head = head.substring(0, maxCharsPerSegment);
      }
      result.add(head);
    }
    appendColumnSegments(result, table, maxSegments, maxCharsPerSegment);
    return result;
  }

  private static void appendColumnSegments(List<String> result, Table table, int maxSegments,
      int maxCharsPerSegment) {
    List<String> columnParts = commentedColumnParts(table);
    StringBuilder batch = new StringBuilder();
    for (String columnPart : columnParts) {
      if (result.size() >= maxSegments) {
        return;
      }
      if (batch.isEmpty()) {
        if (columnPart.length() <= maxCharsPerSegment) {
          batch.append(columnPart);
        } else {
          result.add(columnPart.substring(0, maxCharsPerSegment));
        }
        continue;
      }
      String candidate = batch + "; " + columnPart;
      if (candidate.length() <= maxCharsPerSegment) {
        batch.setLength(0);
        batch.append(candidate);
      } else {
        result.add(batch.toString());
        batch.setLength(0);
        if (result.size() >= maxSegments) {
          return;
        }
        if (columnPart.length() <= maxCharsPerSegment) {
          batch.append(columnPart);
        } else {
          result.add(columnPart.substring(0, maxCharsPerSegment));
        }
      }
    }
    if (result.size() < maxSegments && !batch.isEmpty()) {
      result.add(batch.toString());
    }
  }

  private static String buildHead(String tableName, String comment) {
    return "table: " + tableName.toLowerCase(Locale.ROOT) + "; comment: " + comment;
  }

  private static String tableComment(Table table) {
    if (table.getParameters() == null) {
      return "";
    }
    return StringUtils.defaultString(table.getParameters().get("comment"));
  }

  private static List<String> commentedColumnParts(Table table) {
    List<String> parts = new ArrayList<>();
    if (table.getSd() == null || table.getSd().getCols() == null) {
      return parts;
    }
    for (FieldSchema column : table.getSd().getCols()) {
      if (StringUtils.isNotEmpty(column.getComment())) {
        parts.add("column " + column.getName().toLowerCase(Locale.ROOT) + ": " + column.getComment());
      }
    }
    return parts;
  }
}
