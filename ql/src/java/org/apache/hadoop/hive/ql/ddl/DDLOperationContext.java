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

package org.apache.hadoop.hive.ql.ddl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatter;

/**
 * Context for DDL operations.
 */
public class DDLOperationContext {
  private final Hive db;
  private final HiveConf conf;
  private final DriverContext driverContext;
  private final MetaDataFormatter formatter;

  public DDLOperationContext(HiveConf conf, DriverContext driverContext) throws HiveException {
    this.db = Hive.get(conf);
    this.conf = conf;
    this.driverContext = driverContext;
    this.formatter = MetaDataFormatUtils.getFormatter(conf);
  }

  public Hive getDb() {
    return db;
  }

  public HiveConf getConf() {
    return conf;
  }

  public DriverContext getDriverContext() {
    return driverContext;
  }

  public MetaDataFormatter getFormatter() {
    return formatter;
  }
}
