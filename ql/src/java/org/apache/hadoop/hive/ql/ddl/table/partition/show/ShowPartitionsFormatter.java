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

package org.apache.hadoop.hive.ql.ddl.table.partition.show;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.formatting.MapBuilder;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Formats SHOW PARTITIONS results.
 */
abstract class ShowPartitionsFormatter {
  static ShowPartitionsFormatter getFormatter(HiveConf conf) {
    if (MetaDataFormatUtils.isJson(conf)) {
      return new JsonShowPartitionsFormatter();
    } else {
      return new TextShowPartitionsFormatter();
    }
  }

  abstract void showTablePartitions(DataOutputStream out, List<String> partitions) throws HiveException;

  // ------ Implementations ------

  static class JsonShowPartitionsFormatter extends ShowPartitionsFormatter {
    @Override
    void showTablePartitions(DataOutputStream out, List<String> partitions) throws HiveException {
      List<Map<String, Object>> partitionData = new ArrayList<>(partitions.size());
      for (String partition : partitions) {
        partitionData.add(makeOneTablePartition(partition));
      }
      ShowUtils.asJson(out, MapBuilder.create().put("partitions", partitionData).build());
    }

    // TODO: This seems like a very wrong implementation.
    private Map<String, Object> makeOneTablePartition(String partition) {
      List<Map<String, Object>> result = new ArrayList<>();

      List<String> names = new ArrayList<String>();
      for (String part : partition.split("/")) {
        String name = part;
        String value = null;
        String[] keyValue = StringUtils.split(part, "=", 2);
        if (keyValue != null) {
          name = keyValue[0];
          if (keyValue.length > 1) {
            try {
              value = URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
            }
          }
        }

        if (value != null) {
          names.add(name + "='" + value + "'");
        } else {
          names.add(name);
        }

        result.add(MapBuilder.create()
            .put("columnName", name)
            .put("columnValue", value)
            .build());
      }

      return MapBuilder.create()
          .put("name", StringUtils.join(names, ","))
          .put("values", result)
          .build();
    }
  }

  static class TextShowPartitionsFormatter extends ShowPartitionsFormatter {
    @Override
    void showTablePartitions(DataOutputStream outStream, List<String> partitions) throws HiveException {
      try {
        for (String partition : partitions) {
          // Partition names are URL encoded. We decode the names unless Hive is configured to use the encoded names.
          SessionState ss = SessionState.get();
          if (ss != null && ss.getConf() != null &&
              !ss.getConf().getBoolVar(HiveConf.ConfVars.HIVE_DECODE_PARTITION_NAME)) {
            outStream.write(partition.getBytes(StandardCharsets.UTF_8));
          } else {
            outStream.write(FileUtils.unescapePathName(partition).getBytes(StandardCharsets.UTF_8));
          }
          outStream.write(Utilities.newLineCode);
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }
  }
}
