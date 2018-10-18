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
package org.apache.hadoop.hive.serde2.lazybinary;

import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeUtils;


/**
 * TestLazyBinarySerDe2.
 *
 */
public class TestLazyBinarySerDe2 extends TestLazyBinarySerDe {

  /**
   * Initialize the LazyBinarySerDe.
   *
   * @param fieldNames
   *          table field names
   * @param fieldTypes
   *          table field types
   * @return the initialized LazyBinarySerDe
   * @throws Throwable
   */
  @Override
  protected AbstractSerDe getSerDe(String fieldNames, String fieldTypes) throws Throwable {
    Properties schema = new Properties();
    schema.setProperty(serdeConstants.LIST_COLUMNS, fieldNames);
    schema.setProperty(serdeConstants.LIST_COLUMN_TYPES, fieldTypes);

    LazyBinarySerDe2 serde = new LazyBinarySerDe2();
    SerDeUtils.initializeSerDe(serde, new Configuration(), schema, null);
    return serde;
  }
}
