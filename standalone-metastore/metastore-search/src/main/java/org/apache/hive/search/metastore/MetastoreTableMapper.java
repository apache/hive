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
import java.util.Locale;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.mapping.field.Field;
import org.apache.hive.search.mapping.field.IdField;
import org.apache.hive.search.mapping.field.TextField;

public final class MetastoreTableMapper {
  public static final String FIELD_DB = "db";
  public static final String FIELD_TABLE = "table_name";
  public static final String FIELD_OWNER = "owner";
  public static final String FIELD_TABLE_TYPE = "table_type";
  public static final String FIELD_LOCATION = "location";
  public static final String FIELD_COMMENT = "comment";
  public static final String FIELD_COLUMNS = "columns";
  public static final String FIELD_COLUMN_NAMES = "column_names";
  public static final String FIELD_COLUMN_COMMENTS = "column_comments";
  public static final String FIELD_SEARCH_TEXT = "search_text";
  /** Relative boosts for {@code table_keyword} ranking: table name &gt; column name &gt; comment. */
  public static final float KEYWORD_BOOST_TABLE_NAME = 3.0f;
  public static final float KEYWORD_BOOST_COLUMN_NAME = 2.0f;
  public static final float KEYWORD_BOOST_COMMENT = 1.0f;
  /** Lexical fields used for table keyword search, highest boost first. */
  public static final List<KeywordSearchField> KEYWORD_SEARCH_FIELDS = List.of(
      new KeywordSearchField(FIELD_TABLE, KEYWORD_BOOST_TABLE_NAME),
      new KeywordSearchField(FIELD_COLUMN_NAMES, KEYWORD_BOOST_COLUMN_NAME),
      new KeywordSearchField(FIELD_COMMENT, KEYWORD_BOOST_COMMENT),
      new KeywordSearchField(FIELD_COLUMN_COMMENTS, KEYWORD_BOOST_COMMENT));
  /** Max commented data columns included in search-oriented column fields. */
  static final int MAX_SEARCH_COLUMNS = 100;

  public record KeywordSearchField(String field, float boost) {}

  private MetastoreTableMapper() {}

  public static String tableId(String catalog, String db, String table) {
    return catalog + "." + db + "." + table;
  }

  public static String tableId(TableName tableName) {
    return tableId(tableName.getCat(), tableName.getDb(), tableName.getTable());
  }

  public static TableDocument fromTable(Table table, IndexMapping indexMapping) {
    String db = table.getDbName();
    String name = table.getTableName();
    String catalog = table.getCatName();
    String id = tableId(catalog, db, name);
    String owner = nullToEmpty(table.getOwner());
    String tableType = nullToEmpty(table.getTableType());
    String location = tableLocation(table);
    String comment = tableComment(table);
    String columns = formatColumnsForStorage(table);
    String columnNames = formatColumnNamesForSearch(table);
    String columnComments = formatColumnCommentsForSearch(table);
    String searchText = buildSearchText(name, comment, formatColumnsForSearch(table));

    List<Field> fields = new ArrayList<>(10);
    fields.add(new TextField(FIELD_DB, db));
    fields.add(new TextField(FIELD_TABLE, name.toLowerCase(Locale.ROOT)));
    fields.add(new TextField(FIELD_OWNER, owner));
    fields.add(new TextField(FIELD_TABLE_TYPE, tableType));
    fields.add(new TextField(FIELD_LOCATION, location));
    fields.add(new TextField(FIELD_COMMENT, comment));
    fields.add(new TextField(FIELD_COLUMNS, columns));
    fields.add(new TextField(FIELD_COLUMN_NAMES, columnNames));
    fields.add(new TextField(FIELD_COLUMN_COMMENTS, columnComments));
    fields.add(new TextField(FIELD_SEARCH_TEXT, searchText));
    return new TableDocument(new IdField("_id", id), fields, indexMapping);
  }

  /** Returns true when an alter would change any value written to the search index. */
  public static boolean hasIndexedFieldsChanged(Table before, Table after) {
    Objects.requireNonNull(before);
    Objects.requireNonNull(after);
    if (!Objects.equals(before.getCatName(), after.getCatName())
        || !Objects.equals(before.getDbName(), after.getDbName())
        || !Objects.equals(before.getTableName(), after.getTableName())) {
      return true;
    }
    return !nullToEmpty(before.getOwner()).equals(nullToEmpty(after.getOwner()))
        || !nullToEmpty(before.getTableType()).equals(nullToEmpty(after.getTableType()))
        || !tableLocation(before).equals(tableLocation(after))
        || !tableComment(before).equals(tableComment(after))
        || !formatColumnsForStorage(before).equals(formatColumnsForStorage(after))
        || !formatColumnNamesForSearch(before).equals(formatColumnNamesForSearch(after))
        || !formatColumnCommentsForSearch(before).equals(formatColumnCommentsForSearch(after));
  }

  private static String tableLocation(Table table) {
    return table.getSd() == null ? "" : nullToEmpty(table.getSd().getLocation());
  }

  private static String tableComment(Table table) {
    if (table.getParameters() == null || table.getParameters().get("comment") == null) {
      return "";
    }
    return nullToEmpty(table.getParameters().get("comment"));
  }

  private static String buildSearchText(String tableName, String comment, String searchColumns) {
    String normalizedTableName = tableName.toLowerCase(Locale.ROOT);
    if (comment.isEmpty()) {
      if (searchColumns.isEmpty()) {
        return normalizedTableName;
      }
      return normalizedTableName + " " + searchColumns;
    }
    if (searchColumns.isEmpty()) {
      return normalizedTableName + " " + comment;
    }
    return normalizedTableName + " " + comment + " " + searchColumns;
  }

  private static String formatColumnsForStorage(Table table) {
    if (table.getSd() == null || table.getSd().getCols() == null) {
      return "";
    }
    List<String> parts = new ArrayList<>(table.getSd().getCols().size());
    for (FieldSchema column : table.getSd().getCols()) {
      parts.add(formatColumnForStorage(column));
    }
    return String.join("; ", parts);
  }

  private static String formatColumnNamesForSearch(Table table) {
    if (table.getSd() == null || table.getSd().getCols() == null) {
      return "";
    }
    List<String> names = new ArrayList<>(table.getSd().getCols().size());
    for (FieldSchema column : table.getSd().getCols()) {
      names.add(column.getName().toLowerCase(Locale.ROOT));
    }
    return String.join(" ", names);
  }

  private static String formatColumnCommentsForSearch(Table table) {
    if (table.getSd() == null || table.getSd().getCols() == null) {
      return "";
    }
    List<FieldSchema> commented = new ArrayList<>();
    for (FieldSchema column : table.getSd().getCols()) {
      if (StringUtils.isNotEmpty(column.getComment())) {
        commented.add(column);
      }
    }
    int limit = Math.min(commented.size(), MAX_SEARCH_COLUMNS);
    List<String> parts = new ArrayList<>(limit);
    for (int i = 0; i < limit; i++) {
      parts.add(commented.get(i).getComment());
    }
    String formatted = String.join("; ", parts);
    if (commented.size() > MAX_SEARCH_COLUMNS) {
      return formatted + "; ... (+" + (commented.size() - MAX_SEARCH_COLUMNS) + " more)";
    }
    return formatted;
  }

  private static String formatColumnsForSearch(Table table) {
    if (table.getSd() == null || table.getSd().getCols() == null) {
      return "";
    }
    List<FieldSchema> commented = new ArrayList<>();
    for (FieldSchema column : table.getSd().getCols()) {
      if (StringUtils.isNotEmpty(column.getComment())) {
        commented.add(column);
      }
    }
    int limit = Math.min(commented.size(), MAX_SEARCH_COLUMNS);
    List<String> parts = new ArrayList<>(limit);
    for (int i = 0; i < limit; i++) {
      parts.add(formatColumnForSearch(commented.get(i)));
    }
    String formatted = String.join("; ", parts);
    if (commented.size() > MAX_SEARCH_COLUMNS) {
      return formatted + "; ... (+" + (commented.size() - MAX_SEARCH_COLUMNS) + " more)";
    }
    return formatted;
  }

  private static String formatColumnForStorage(FieldSchema column) {
    String base = column.getName() + " " + column.getType();
    if (StringUtils.isNotEmpty(column.getComment())) {
      return base + " " + column.getComment();
    }
    return base;
  }

  private static String formatColumnForSearch(FieldSchema column) {
    return column.getName() + " " + column.getComment();
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }
}
