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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.parse.rewrite.sql;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;

import static org.apache.hadoop.hive.ql.metadata.RowLineageUtils.addSourceColumnsForRowLineage;

public class COWWithClauseBuilder {

  private static final String MARKER_CTE_BASE_NAME = "hive_cow_markers";
  private static final String MATCHED_CTE_BASE_NAME = "hive_cow_matched";
  private static final String ROW_NUMBER_BASE_NAME = "hive_cow_row_number";
  private static final String FILE_PATH_BASE_NAME = "hive_cow_file_path";
  private static final String MATCHED_ROW_COUNT_BASE_NAME = "hive_cow_matched_row_count";
  private static final String MATCHED_ROW_MARKER = "-%s";
  private String markerCteName;

  public void appendWith(MultiInsertSqlGenerator sqlGenerator, String filePathCol, String whereClause) {
    appendWith(sqlGenerator, null, filePathCol, whereClause, true);
  }

  public void appendWith(MultiInsertSqlGenerator sqlGenerator, String sourceName, String filePathCol,
      String whereClause, boolean skipPrefix) {
    appendWith(sqlGenerator, sourceName, filePathCol, whereClause, skipPrefix, false, "");
  }

  public void appendWith(MultiInsertSqlGenerator sqlGenerator, String sourceName, String filePathCol,
      String whereClause, boolean skipPrefix, boolean isRowLineageSupported, String rowLineagePrefix) {
    sqlGenerator.newCteExpr();

    markerCteName = uniqueCteName(MARKER_CTE_BASE_NAME, sqlGenerator, sourceName, whereClause);
    String matchedCteName = uniqueCteName(MATCHED_CTE_BASE_NAME, sqlGenerator, sourceName, whereClause);
    Set<String> reservedAliases = getReservedAliases(sqlGenerator);
    String rowNumberCol = quote(uniqueAlias(ROW_NUMBER_BASE_NAME, reservedAliases), sqlGenerator);
    String filePathAlias = quote(uniqueAlias(FILE_PATH_BASE_NAME, reservedAliases), sqlGenerator);
    String matchedRowCountCol = quote(uniqueAlias(MATCHED_ROW_COUNT_BASE_NAME, reservedAliases), sqlGenerator);
    String source = sourceName == null ? sqlGenerator.getTargetTableFullName() : sourceName;

    sqlGenerator.append(matchedCteName).append(" AS (");
    sqlGenerator.append("\n").indent();
    sqlGenerator.append("select ");
    sqlGenerator.appendAcidSelectColumnsForDeletedRecords(Context.Operation.DELETE, skipPrefix);
    sqlGenerator.removeLastChar();
    addSourceColumnsForRowLineage(isRowLineageSupported, sqlGenerator, rowLineagePrefix, sqlGenerator.conf);
    sqlGenerator.append(" from ").append(source);
    sqlGenerator.append("\n").indent();
    sqlGenerator.append("where (").append(whereClause).append(") AND ")
        .append(filePathCol).append(" IS NOT NULL\n),\n");

    sqlGenerator.append(markerCteName).append(" AS (");
    sqlGenerator.append("\n").indent();
    sqlGenerator.append("select ");
    // The negative row id encodes the number of matched rows in the replaced file.
    sqlGenerator.appendAcidSelectColumnsForDeletedRecords(
        Context.Operation.DELETE, skipPrefix, String.format(MATCHED_ROW_MARKER, "c." + matchedRowCountCol));
    sqlGenerator.removeLastChar();
    addSourceColumnsForRowLineage(isRowLineageSupported, sqlGenerator, rowLineagePrefix, sqlGenerator.conf);
    sqlGenerator.append(" from (");
    sqlGenerator.append("\n").indent().indent();
    sqlGenerator.append("select ");
    sqlGenerator.appendAcidSelectColumnsForDeletedRecords(Context.Operation.DELETE, skipPrefix);
    sqlGenerator.removeLastChar();
    addSourceColumnsForRowLineage(isRowLineageSupported, sqlGenerator, rowLineagePrefix, sqlGenerator.conf);
    sqlGenerator.append(", row_number() OVER (partition by ").append(filePathCol).append(") AS ")
        .append(rowNumberCol);
    sqlGenerator.append(" from ").append(matchedCteName);
    sqlGenerator.append("\n").indent();
    sqlGenerator.append(") q");
    sqlGenerator.append("\n").indent();
    sqlGenerator.append("join (");
    sqlGenerator.append("\n").indent().indent();
    sqlGenerator.append("select ").append(filePathCol).append(" AS ").append(filePathAlias);
    sqlGenerator.append(", count(*) AS ").append(matchedRowCountCol);
    sqlGenerator.append(" from ").append(matchedCteName);
    sqlGenerator.append("\n").indent().indent();
    sqlGenerator.append("group by ").append(filePathCol);
    sqlGenerator.append("\n").indent();
    sqlGenerator.append(") c on q.").append(filePathCol).append(" = c.").append(filePathAlias);
    sqlGenerator.append("\n").indent();
    sqlGenerator.append("where q.").append(rowNumberCol).append("=1\n)\n");

    sqlGenerator.addCteExpr();
  }

  public String getMarkerCteName() {
    return markerCteName;
  }

  private static String quote(String name, MultiInsertSqlGenerator sqlGenerator) {
    return HiveUtils.unparseIdentifier(name, sqlGenerator.conf);
  }

  private static String uniqueCteName(String baseName, MultiInsertSqlGenerator sqlGenerator, String sourceName,
      String whereClause) {
    int suffix = 0;
    String candidate;
    do {
      candidate = baseName + (suffix == 0 ? "" : "_" + suffix);
      suffix++;
    } while (sameRelationName(candidate, sqlGenerator.getTargetTableFullName())
        || sameRelationName(candidate, sourceName)
        || whereClause.toLowerCase(Locale.ROOT).contains(candidate.toLowerCase(Locale.ROOT)));
    return candidate;
  }

  private static boolean sameRelationName(String name, String relation) {
    if (relation == null) {
      return false;
    }
    String unquoted = relation.replace("`", "");
    int dot = unquoted.lastIndexOf('.');
    return name.equalsIgnoreCase(dot < 0 ? unquoted : unquoted.substring(dot + 1));
  }

  private static Set<String> getReservedAliases(MultiInsertSqlGenerator sqlGenerator) {
    Set<String> reservedAliases = Stream.concat(
        sqlGenerator.getTargetTable().getAllCols().stream(),
        sqlGenerator.getTargetTable().getStorageHandler()
            .acidSelectColumns(sqlGenerator.getTargetTable(), Context.Operation.DELETE).stream())
        .map(column -> column.getName().toLowerCase(Locale.ROOT))
        .collect(Collectors.toCollection(HashSet::new));
    reservedAliases.add(VirtualColumn.ROW_LINEAGE_ID.getName().toLowerCase(Locale.ROOT));
    reservedAliases.add(VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER.getName().toLowerCase(Locale.ROOT));
    return reservedAliases;
  }

  private static String uniqueAlias(String baseAlias, Set<String> reservedAliases) {
    String alias = baseAlias;
    int suffix = 0;
    while (!reservedAliases.add(alias.toLowerCase(Locale.ROOT))) {
      suffix++;
      alias = baseAlias + "_" + suffix;
    }
    return alias;
  }
}
