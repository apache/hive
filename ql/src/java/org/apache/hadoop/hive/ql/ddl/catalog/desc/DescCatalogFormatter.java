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

package org.apache.hadoop.hive.ql.ddl.catalog.desc;

import org.apache.hadoop.hive.common.type.CalendarUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.formatting.MapBuilder;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hive.common.util.HiveStringUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Formats DESC CATALOG results.
 */
abstract class DescCatalogFormatter {
  static DescCatalogFormatter getFormatter(HiveConf hiveConf) {
    if (MetaDataFormatUtils.isJson(hiveConf)) {
      return new JsonDescCatalogFormatter();
    }
    return new TextDescCatalogFormatter();
  }

  abstract void showCatalogDescription(DataOutputStream out, String catalog, String comment, String location,
      int createTime) throws HiveException;

  // ------ Implementations ------
  static class JsonDescCatalogFormatter extends DescCatalogFormatter {
    @Override
    void showCatalogDescription(DataOutputStream out, String catalog, String comment, String location,
        int createTime) throws HiveException {
      MapBuilder builder = MapBuilder.create()
          .put("catalog", catalog)
          .put("comment", comment)
          .put("location", location);
      if (createTime != 0) {
        builder.put("createTime", CalendarUtils.formatTimestamp((long) createTime * 1000, true));
      }
      ShowUtils.asJson(out, builder.build());
    }
  }

  static class TextDescCatalogFormatter extends DescCatalogFormatter {
    @Override
    void showCatalogDescription(DataOutputStream out, String catalog, String comment, String location,
        int createTime) throws HiveException {
      try {
        writeLine(out, "Catalog Name", catalog);
        if (comment != null) {
          writeLine(out, "Comment", HiveStringUtils.escapeJava(comment));
        }
        if (location != null) {
          writeLine(out, "Location", location);
        }
        if (createTime != 0) {
          String createTimeStr = CalendarUtils.formatTimestamp((long) createTime * 1000, true);
          writeLine(out, "CreateTime", createTimeStr);
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }

    private void writeLine(DataOutputStream out, String label, String value) throws IOException {
      out.write(label.getBytes(StandardCharsets.UTF_8));
      out.write(Utilities.tabCode);
      out.write(value.getBytes(StandardCharsets.UTF_8));
      out.write(Utilities.newLineCode);
    }
  }
}
