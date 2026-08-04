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

package org.apache.hadoop.hive.ql.parse.type;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestAmbiguousColumnName {

  private static ColumnInfo colInfo() {
    ColumnInfo colInfo = new ColumnInfo("_col0", TypeInfoFactory.stringTypeInfo, "t", false);
    colInfo.setAlias("c");
    return colInfo;
  }

  @Test
  public void testCopyConstructorPreservesAmbiguousName() {
    ColumnInfo original = colInfo();
    original.setAmbiguousName(true);
    Assert.assertTrue(new ColumnInfo(original).hasAmbiguousName());
  }

  @Test
  public void testCheckAmbiguousNameThrows() {
    ColumnInfo marked = colInfo();
    marked.setAmbiguousName(true);
    try {
      TypeCheckProcFactory.checkAmbiguousName(marked);
      Assert.fail("expected SemanticException");
    } catch (SemanticException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Ambiguous column reference c in t"));
    }
  }

  @Test
  public void testCheckAmbiguousNameNoThrow() throws SemanticException {
    TypeCheckProcFactory.checkAmbiguousName(colInfo());
  }
}
