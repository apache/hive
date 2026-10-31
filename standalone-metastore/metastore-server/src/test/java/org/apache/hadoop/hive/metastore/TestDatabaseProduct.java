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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestDatabaseProduct {

  @Test
  public void testCountsChangedRowsOnly() {
    Assert.assertTrue(DatabaseProduct.countsChangedRowsOnly("jdbc:mysql://host:3306/hms?useAffectedRows=true"));
    Assert.assertTrue(DatabaseProduct.countsChangedRowsOnly("jdbc:mariadb://host:3306/hms?useAffectedRows=true"));
    Assert.assertFalse(DatabaseProduct.countsChangedRowsOnly("jdbc:mysql://host:3306/hms"));
    Assert.assertFalse(DatabaseProduct.countsChangedRowsOnly("jdbc:postgresql://host:5432/hms?useAffectedRows=true"));
  }
}
