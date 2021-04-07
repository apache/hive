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

package org.apache.hadoop.hive.ql.ddl.database.desc;

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
 * Formats DESC DATABASES results.
 */
abstract class DescDatabaseFormatter {
  static DescDatabaseFormatter getFormatter(HiveConf conf) {
    if (MetaDataFormatUtils.isJson(conf)) {
      return new JsonDescDatabaseFormatter();
    } else {
      return new TextDescDatabaseFormatter();
    }
  }

  abstract void showDatabaseDescription(DataOutputStream out, String database, String comment, String location,
      String managedLocation, String ownerName, PrincipalType ownerType, Map<String, String> params,
      String connectorName, String remoteDbName)
      throws HiveException;

  // ------ Implementations ------

  static class JsonDescDatabaseFormatter extends DescDatabaseFormatter {
    @Override
    void showDatabaseDescription(DataOutputStream out, String database, String comment, String location,
        String managedLocation, String ownerName, PrincipalType ownerType, Map<String, String> params,
        String connectorName, String remoteDbName)
        throws HiveException {
      MapBuilder builder = MapBuilder.create()
          .put("database", database)
          .put("comment", comment)
          .put("location", location);
      if (managedLocation != null) {
        builder.put("managedLocation", managedLocation);
      }
      if (ownerName != null) {
        builder.put("owner", ownerName);
      }
      if (ownerType != null) {
        builder.put("ownerType", ownerType.name());
      }
      if (connectorName != null) {
        builder.put("connector_name", connectorName);
      }
      if (remoteDbName != null) {
        builder.put("remote_dbname", remoteDbName);
      }
      if (MapUtils.isNotEmpty(params)) {
        builder.put("params", params);
      }
      ShowUtils.asJson(out, builder.build());
    }
  }

  static class TextDescDatabaseFormatter extends DescDatabaseFormatter {
    @Override
    void showDatabaseDescription(DataOutputStream out, String database, String comment, String location,
        String managedLocation, String ownerName, PrincipalType ownerType, Map<String, String> params,
        String connectorName, String remoteDbName)
        throws HiveException {
      try {
        out.write(database.getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.tabCode);
        if (comment != null) {
          out.write(HiveStringUtils.escapeJava(comment).getBytes(StandardCharsets.UTF_8));
        }
        out.write(Utilities.tabCode);
        if (location != null) {
          out.write(location.getBytes(StandardCharsets.UTF_8));
        }
        out.write(Utilities.tabCode);
        if (managedLocation != null) {
          out.write(managedLocation.getBytes(StandardCharsets.UTF_8));
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
        if (connectorName != null) {
          out.write(connectorName.getBytes(StandardCharsets.UTF_8));
        }
        out.write(Utilities.tabCode);
        if (remoteDbName != null) {
          out.write(remoteDbName.getBytes(StandardCharsets.UTF_8));
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
