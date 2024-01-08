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

package org.apache.hadoop.hive.ql.ddl.view.materialized.show;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.ddl.ShowUtils.TextMetaDataTable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.jetbrains.annotations.NotNull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.conf.Constants.MATERIALIZED_VIEW_REWRITING_TIME_WINDOW;
import static org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization.IncrementalRebuildMode.UNKNOWN;
import static org.apache.hadoop.hive.ql.metadata.RewriteAlgorithm.ALL;

/**
 * Formats SHOW MATERIALIZED VIEWS results.
 */
abstract class ShowMaterializedViewsFormatter {
  static ShowMaterializedViewsFormatter getFormatter(HiveConf conf) {
    if (MetaDataFormatUtils.isJson(conf)) {
      return new JsonShowMaterializedViewsFormatter();
    } else {
      return new TextShowMaterializedViewsFormatter();
    }
  }

  abstract void showMaterializedViews(DataOutputStream out, List<Table> materializedViews) throws HiveException;

  private static String getMode(Table materializedView) {
    // TODO: Currently, we only support manual refresh, we should update whenever we have other modes
    String refreshMode = "Manual refresh";
    String timeWindowString = materializedView.getProperty(MATERIALIZED_VIEW_REWRITING_TIME_WINDOW);
    String mode;
    if (!StringUtils.isEmpty(timeWindowString)) {
      long time = HiveConf.toTime(timeWindowString,
          HiveConf.getDefaultTimeUnit(HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_TIME_WINDOW),
          TimeUnit.MINUTES);
      if (time > 0L) {
        mode = refreshMode + " (Valid for " + time + "min)";
      } else if (time == 0L) {
        mode = refreshMode + " (Valid until source tables modified)";
      } else {
        mode = refreshMode + " (Valid always)";
      }
    } else {
      mode = refreshMode;
    }
    return mode;
  }

  // ------ Implementations ------

  static class JsonShowMaterializedViewsFormatter extends ShowMaterializedViewsFormatter {
    @Override
    void showMaterializedViews(DataOutputStream out, List<Table> materializedViews) throws HiveException {
      if (materializedViews.isEmpty()) {
        return;
      }

      List<Map<String, Object>> materializedViewDataList = new ArrayList<>();
      for (Table materializedView : materializedViews) {
        String name = materializedView.getTableName();
        String rewriteEnabled = materializedView.isRewriteEnabled() ? "Yes" : "No";
        String mode = getMode(materializedView);
        Map<String, Object> materializedViewData = ImmutableMap.of(
            "MV Name", name,
            "Rewriting Enabled", rewriteEnabled,
            "Mode", mode,
            "Incremental rebuild", ShowMaterializedViewsFormatter.formatIncrementalRebuildMode(materializedView));
        materializedViewDataList.add(materializedViewData);
      }
      ShowUtils.asJson(out, ImmutableMap.of("materialized views", materializedViewDataList));
    }
  }

  static class TextShowMaterializedViewsFormatter extends ShowMaterializedViewsFormatter {
    @Override
    void showMaterializedViews(DataOutputStream out, List<Table> materializedViews) throws HiveException {
      if (materializedViews.isEmpty()) {
        return;
      }

      try {
        TextMetaDataTable mdt = new TextMetaDataTable();
        if (!SessionState.get().isHiveServerQuery()) {
          mdt.addRow("# MV Name", "Rewriting Enabled", "Mode", "Incremental rebuild");
        }
        for (Table materializedView : materializedViews) {
          String name = materializedView.getTableName();
          String rewriteEnabled = materializedView.isRewriteEnabled() ? "Yes" : "No";
          String mode = getMode(materializedView);

          String incrementalRebuild = formatIncrementalRebuildMode(materializedView);

          mdt.addRow(name, rewriteEnabled, mode, incrementalRebuild);
        }
        // In case the query is served by HiveServer2, don't pad it with spaces,
        // as HiveServer2 output is consumed by JDBC/ODBC clients.
        out.write(mdt.renderTable(!SessionState.get().isHiveServerQuery()).getBytes(StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }
  }

  @NotNull
  private static String formatIncrementalRebuildMode(Table materializedView) {
    String incrementalRebuild;
    HiveRelOptMaterialization relOptMaterialization = HiveMaterializedViewsRegistry.get().
            getRewritingMaterializedView(materializedView.getDbName(), materializedView.getTableName(), ALL);
    if (relOptMaterialization == null || relOptMaterialization.getRebuildMode() == UNKNOWN) {
      incrementalRebuild = "Unknown";
    } else {
      switch (relOptMaterialization.getRebuildMode()) {
        case AVAILABLE:
          incrementalRebuild = "Available";
          break;
        case INSERT_ONLY:
          incrementalRebuild = "Available in presence of insert operations only";
          break;
        case NOT_AVAILABLE:
          incrementalRebuild = "Not available";
          break;
        default:
          incrementalRebuild = "Unknown";
          break;
      }
    }
    return incrementalRebuild;
  }
}
