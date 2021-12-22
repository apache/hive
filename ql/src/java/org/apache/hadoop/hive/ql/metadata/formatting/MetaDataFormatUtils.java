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

package org.apache.hadoop.hive.ql.metadata.formatting;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * This class provides methods to format table and index information.
 *
 */
public final class MetaDataFormatUtils {
  private MetaDataFormatUtils() {
    throw new UnsupportedOperationException("MetaDataFormatUtils should not be instantiated");
  }

  public static MetaDataFormatter getFormatter(HiveConf conf) {
    if (isJson(conf)) {
      return new JsonMetaDataFormatter();
    } else {
      return new TextMetaDataFormatter();
    }
  }

  public static boolean isJson(HiveConf conf) {
    return "json".equals(conf.get(HiveConf.ConfVars.HIVE_DDL_OUTPUT_FORMAT.varname, "text"));
  }
}
