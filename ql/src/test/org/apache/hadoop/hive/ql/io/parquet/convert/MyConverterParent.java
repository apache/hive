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

package org.apache.hadoop.hive.ql.io.parquet.convert;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.Writable;

/**
 * Helper class for TestETypeConverter.
 */
public class MyConverterParent implements ConverterParent {

  private Writable value;

  public Writable getValue() {
    return value;
  }

  @Override
  public void set(int index, Writable value) {
    this.value = value;
  }

  @Override
  public Map<String, String> getMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION.varname, "false");
    metadata.put(DataWritableWriteSupport.WRITER_ZONE_CONVERSION_LEGACY, "false");
    return metadata;
  }

}
