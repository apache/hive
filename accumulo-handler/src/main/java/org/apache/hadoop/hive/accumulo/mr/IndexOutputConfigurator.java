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

package org.apache.hadoop.hive.accumulo.mr;

import org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator;
import org.apache.hadoop.conf.Configuration;

/**
 * Extension of OutputConfigurtion to support indexing.
 */
public class IndexOutputConfigurator extends OutputConfigurator {
  /**
   * Accumulo Write options.
   */
  public static enum WriteOpts {
    DEFAULT_TABLE_NAME,
    INDEX_TABLE_NAME,
    INDEX_COLUMNS,
    COLUMN_TYPES,
    BINARY_ENCODING,
    BATCH_WRITER_CONFIG;

    private WriteOpts() {
    }
  }

  public static void setIndexTableName(Class<?> implementingClass, Configuration conf,
                                       String tableName) {
    if(tableName != null) {
      conf.set(enumToConfKey(implementingClass, WriteOpts.INDEX_TABLE_NAME), tableName);
    }
  }

  public static String getIndexTableName(Class<?> implementingClass, Configuration conf) {
    return conf.get(enumToConfKey(implementingClass, WriteOpts.INDEX_TABLE_NAME));
  }

  public static void setIndexColumns(Class<?> implementingClass, Configuration conf,
                                     String tableName) {
    if(tableName != null) {
      conf.set(enumToConfKey(implementingClass, WriteOpts.INDEX_COLUMNS), tableName);
    }
  }

  public static String getIndexColumns(Class<?> implementingClass, Configuration conf) {
    return conf.get(enumToConfKey(implementingClass, WriteOpts.INDEX_COLUMNS));
  }


  public static void setRecordEncoding(Class<?> implementingClass, Configuration conf,
                                       Boolean isBinary) {
      conf.set(enumToConfKey(implementingClass, WriteOpts.BINARY_ENCODING), isBinary.toString());
  }

  public static Boolean getRecordEncoding(Class<?> implementingClass, Configuration conf) {
    return Boolean.valueOf(conf.get(enumToConfKey(implementingClass, WriteOpts.BINARY_ENCODING)));
  }

}
