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
package org.apache.hadoop.hive.accumulo.columns;

import java.util.Map.Entry;

import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 *
 */
public class ColumnMappingFactory {
  private static final Logger log = LoggerFactory.getLogger(ColumnMappingFactory.class);

  /**
   * Generate the proper instance of a ColumnMapping
   *
   * @param columnSpec
   *          Specification for mapping this column to Accumulo
   * @param defaultEncoding
   *          The default encoding in which values should be encoded to Accumulo
   */
  public static ColumnMapping get(String columnSpec, ColumnEncoding defaultEncoding,
      String columnName, TypeInfo columnType) {
    Preconditions.checkNotNull(columnSpec);
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(columnType);
    ColumnEncoding encoding = defaultEncoding;

    // Check for column encoding specification
    if (ColumnEncoding.hasColumnEncoding(columnSpec)) {
      String columnEncodingStr = ColumnEncoding.getColumnEncoding(columnSpec);
      columnSpec = ColumnEncoding.stripCode(columnSpec);

      if (AccumuloHiveConstants.ROWID.equalsIgnoreCase(columnSpec)) {
        return new HiveAccumuloRowIdColumnMapping(columnSpec,
            ColumnEncoding.get(columnEncodingStr), columnName, columnType.getTypeName());
      } else {
        Entry<String,String> pair = parseMapping(columnSpec);

        if (isPrefix(pair.getValue())) {
          // Sanity check that, for a map, we got 2 encodings
          if (!ColumnEncoding.isMapEncoding(columnEncodingStr)) {
            throw new IllegalArgumentException("Expected map encoding for a map specification, "
                + columnSpec + " with encoding " + columnEncodingStr);
          }

          Entry<ColumnEncoding,ColumnEncoding> encodings = ColumnEncoding
              .getMapEncoding(columnEncodingStr);

          return new HiveAccumuloMapColumnMapping(pair.getKey(), pair.getValue(),
              encodings.getKey(), encodings.getValue(), columnName, columnType.getTypeName());
        } else {
          return new HiveAccumuloColumnMapping(pair.getKey(), pair.getValue(),
              ColumnEncoding.getFromMapping(columnEncodingStr), columnName, columnType.getTypeName());
        }
      }
    } else {
      if (AccumuloHiveConstants.ROWID.equalsIgnoreCase(columnSpec)) {
        return new HiveAccumuloRowIdColumnMapping(columnSpec, defaultEncoding, columnName,
            columnType.getTypeName());
      } else {
        Entry<String,String> pair = parseMapping(columnSpec);
        boolean isPrefix = isPrefix(pair.getValue());

        String cq = pair.getValue();

        // Replace any \* that appear in the prefix with a regular *
        if (-1 != cq.indexOf(AccumuloHiveConstants.ESCAPED_ASTERISK)) {
          cq = cq.replaceAll(AccumuloHiveConstants.ESCAPED_ASERTISK_REGEX,
              Character.toString(AccumuloHiveConstants.ASTERISK));
        }

        if (isPrefix) {
          return new HiveAccumuloMapColumnMapping(pair.getKey(), cq.substring(0, cq.length() - 1),
              defaultEncoding, defaultEncoding, columnName, columnType.getTypeName());
        } else {
          return new HiveAccumuloColumnMapping(pair.getKey(), cq, encoding, columnName, columnType.getTypeName());
        }
      }
    }
  }

  public static ColumnMapping getMap(String columnSpec, ColumnEncoding keyEncoding,
      ColumnEncoding valueEncoding, String columnName, TypeInfo columnType) {
    Entry<String,String> pair = parseMapping(columnSpec);
    return new HiveAccumuloMapColumnMapping(pair.getKey(), pair.getValue(), keyEncoding,
        valueEncoding, columnName, columnType.toString());

  }

  public static boolean isPrefix(String maybePrefix) {
    Preconditions.checkNotNull(maybePrefix);

    if (AccumuloHiveConstants.ASTERISK == maybePrefix.charAt(maybePrefix.length() - 1)) {
      if (maybePrefix.length() > 1) {
        return AccumuloHiveConstants.ESCAPE != maybePrefix.charAt(maybePrefix.length() - 2);
      } else {
        return true;
      }
    }

    // If we couldn't find an asterisk, it's not a prefix
    return false;
  }

  /**
   * Consumes the column mapping specification and breaks it into column family and column
   * qualifier.
   */
  public static Entry<String,String> parseMapping(String columnSpec)
      throws InvalidColumnMappingException {
    int index = 0;
    while (true) {
      if (index >= columnSpec.length()) {
        log.error("Cannot parse '" + columnSpec + "' as colon-separated column configuration");
        throw new InvalidColumnMappingException(
            "Columns must be provided as colon-separated family and qualifier pairs");
      }

      index = columnSpec.indexOf(AccumuloHiveConstants.COLON, index);

      if (-1 == index) {
        log.error("Cannot parse '" + columnSpec + "' as colon-separated column configuration");
        throw new InvalidColumnMappingException(
            "Columns must be provided as colon-separated family and qualifier pairs");
      }

      // Check for an escape character before the colon
      if (index - 1 > 0) {
        char testChar = columnSpec.charAt(index - 1);
        if (AccumuloHiveConstants.ESCAPE == testChar) {
          // this colon is escaped, search again after it
          index++;
          continue;
        }

        // If the previous character isn't an escape characters, it's the separator
      }

      // Can't be escaped, it is the separator
      break;
    }

    String cf = columnSpec.substring(0, index), cq = columnSpec.substring(index + 1);

    // Check for the escaped colon to remove before doing the expensive regex replace
    if (-1 != cf.indexOf(AccumuloHiveConstants.ESCAPED_COLON)) {
      cf = cf.replaceAll(AccumuloHiveConstants.ESCAPED_COLON_REGEX,
          Character.toString(AccumuloHiveConstants.COLON));
    }

    // Check for the escaped colon to remove before doing the expensive regex replace
    if (-1 != cq.indexOf(AccumuloHiveConstants.ESCAPED_COLON)) {
      cq = cq.replaceAll(AccumuloHiveConstants.ESCAPED_COLON_REGEX,
          Character.toString(AccumuloHiveConstants.COLON));
    }

    return Maps.immutableEntry(cf, cq);
  }
}
