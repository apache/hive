/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.columns;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class TestHiveAccumuloColumnMapping {

  @Test
  public void testColumnMappingWithMultipleColons() {
    // A column qualifier with a colon
    String cf = "cf", cq = "cq1:cq2";
    HiveAccumuloColumnMapping mapping = new HiveAccumuloColumnMapping(cf, cq,
        ColumnEncoding.STRING, "col", TypeInfoFactory.stringTypeInfo.toString());

    Assert.assertEquals(cf, mapping.getColumnFamily());
    Assert.assertEquals(cq, mapping.getColumnQualifier());
  }
}
