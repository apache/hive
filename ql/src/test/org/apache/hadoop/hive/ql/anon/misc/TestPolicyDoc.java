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

package org.apache.hadoop.hive.ql.anon.misc;

import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hadoop.hive.ql.anon.TestUtils.getTestPolicy;

public class TestPolicyDoc {

  @Test
  public void test() throws IOException {
    final DataErasurePolicy policy = getTestPolicy();

    Assertions.assertEquals("int", policy.identityFieldType);
    Assertions.assertEquals("int", policy.schemaFieldType);
    Assertions.assertEquals(1, policy.statements.size());
    Assertions.assertEquals(4, policy.statements.get(0).rules.size());
  }

  @Test
  public void stringSchemaIdMatchesAsText() {
    final DataErasurePolicy policy = DataErasurePolicy.fromDsl(
        """
        VERSION v1
        IDENTITY userId TYPE INT
        SCHEMA TYPE STRING
        FOR SCHEMA 'user_info_v1'
            ERASE telephone
        """);

    Assertions.assertEquals("string", policy.schemaFieldType);
    Assertions.assertTrue(policy.hasId(new Text("user_info_v1")),
        "the declared string schema id must match the row's Text value");
    Assertions.assertFalse(policy.hasId(new Text("other_schema")),
        "an unrelated schema id must not match");
    Assertions.assertNotNull(policy.getStmt(new Text("user_info_v1")),
        "getStmt must resolve the statement for the matched string schema id");
    Assertions.assertEquals(1, policy.getStmt(new Text("user_info_v1")).rules.size());
  }

  @Test
  public void intSchemaIdMatchesAsIntWritable() {
    final DataErasurePolicy policy = DataErasurePolicy.fromDsl(
        """
        VERSION v1
        IDENTITY userId TYPE INT
        SCHEMA TYPE INT
        FOR SCHEMA 3
            ERASE telephone
        """);

    Assertions.assertEquals("int", policy.schemaFieldType);
    Assertions.assertTrue(policy.hasId(new IntWritable(3)));
    Assertions.assertFalse(policy.hasId(new IntWritable(4)));
    Assertions.assertNotNull(policy.getStmt(new IntWritable(3)));
  }

}
