package org.apache.hadoop.hive.ql.queryhistory.persist;
/**
 * Licensed to the Apache Software Foundation (ASF) under one§
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistorySchema;

/*
 * A test iceberg persistor which has the same behavior as parent but without any metastore locking
 * (for easier unit test's sake).
 */
public class IcebergPersistorForTest extends IcebergPersistor {

  @Override
  public void init(HiveConf conf, QueryHistorySchema schema) {
    conf.set("iceberg.engine.hive.lock-enabled", "false");
    super.init(conf, schema);
  }
}
