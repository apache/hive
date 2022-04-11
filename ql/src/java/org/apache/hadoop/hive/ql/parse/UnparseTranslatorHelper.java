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

package org.apache.hadoop.hive.ql.parse;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;

/**
 * Class containing static methods that help populate the UnparseTranslator.
 */
public class UnparseTranslatorHelper {

  /**
   * Adds translation to the unparseTranslator for the RexNode if it is a RexInputRef.
   * Grabs the inputRef information from the given RowResolver.
   */
  public static void addTranslationIfNeeded(ASTNode astNode, RexNode rexNode, RowResolver rr,
      UnparseTranslator unparseTranslator, HiveConf conf) {
    if (unparseTranslator == null || !unparseTranslator.isEnabled()) {
      return;
    }
    if (!(rexNode instanceof RexInputRef)) {
      return;
    }
    RexInputRef inputRef = (RexInputRef) rexNode;
    ColumnInfo columnInfo = rr.getColumnInfos().get(inputRef.getIndex());
    if (columnInfo.getTabAlias() == null || columnInfo.getTabAlias().length() == 0) {
      return;
    }
    String[] tmp = rr.reverseLookup(columnInfo.getInternalName());
    StringBuilder replacementText = new StringBuilder();
    replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
    replacementText.append(".");
    replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
    unparseTranslator.addTranslation(astNode, replacementText.toString());
  }
}
