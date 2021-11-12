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

package org.apache.hadoop.hive.ql.ddl.dataconnector.desc;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.formatting.MapBuilder;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hive.common.util.HiveStringUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Formats DESC CONNECTOR results.
 */
abstract class DescDataConnectorFormatter {
  static DescDataConnectorFormatter getFormatter(HiveConf conf) {
    if (MetaDataFormatUtils.isJson(conf)) {
      return new JsonDescDataConnectorFormatter();
    } else {
      return new TextDescDataConnectorFormatter();
    }
  }

  abstract void showDataConnectorDescription(DataOutputStream out, String connector, String type, String url,
      String ownerName, PrincipalType ownerType, String comment, Map<String, String> params)
      throws HiveException;

  // ------ Implementations ------

  static class JsonDescDataConnectorFormatter extends DescDataConnectorFormatter {
    @Override
    void showDataConnectorDescription(DataOutputStream out, String connector, String type, String url,
        String ownerName, PrincipalType ownerType, String comment, Map<String, String> params)
        throws HiveException {
      MapBuilder builder = MapBuilder.create()
          .put("connector", connector)
          .put("type", type)
          .put("url", url);
      if (ownerName != null) {
        builder.put("owner", ownerName);
      }
      if (ownerType != null) {
        builder.put("ownerType", ownerType.name());
      }
      if (comment != null) {
        builder.put("comment", comment);
      }
      if (MapUtils.isNotEmpty(params)) {
        builder.put("params", params);
      }
      ShowUtils.asJson(out, builder.build());
    }
  }

  static class TextDescDataConnectorFormatter extends DescDataConnectorFormatter {
    @Override
    void showDataConnectorDescription(DataOutputStream out, String connector, String type, String url,
        String ownerName, PrincipalType ownerType, String comment, Map<String, String> params)
        throws HiveException {
      try {
        out.write(connector.getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.tabCode);
        if (type != null) {
          out.write(type.getBytes(StandardCharsets.UTF_8));
        }
        out.write(Utilities.tabCode);
        if (url != null) {
          out.write(url.getBytes(StandardCharsets.UTF_8));
        }
        out.write(Utilities.tabCode);
        if (ownerName != null) {
          out.write(ownerName.getBytes(StandardCharsets.UTF_8));
        }
        out.write(Utilities.tabCode);
        if (ownerType != null) {
          out.write(ownerType.name().getBytes(StandardCharsets.UTF_8));
        }
        out.write(Utilities.tabCode);
        if (comment != null) {
          out.write(HiveStringUtils.escapeJava(comment).getBytes(StandardCharsets.UTF_8));
        }
        out.write(Utilities.tabCode);
        if (MapUtils.isNotEmpty(params)) {
          out.write(params.toString().getBytes(StandardCharsets.UTF_8));
        }
        out.write(Utilities.newLineCode);
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }
  }
}
