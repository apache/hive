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
package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlGenerator;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;

import java.util.List;
import java.util.Map;

public class PartitionSplitUpdateRewriter extends SplitUpdateRewriter {

  public PartitionSplitUpdateRewriter(HiveConf conf, SqlGeneratorFactory sqlGeneratorFactory) {
    super(conf, sqlGeneratorFactory);
  }

  @Override
  protected void appendPartitionColumns(UpdateStatement updateBlock, MultiInsertSqlGenerator sqlGenerator,
      List<String> insertValues, Map<Integer, ASTNode> setColExprs,
      int columnOffset) {
    List<FieldSchema> partCols = updateBlock.getTargetTable().getPartCols();
    boolean first = partCols.size() == updateBlock.getTargetTable().getAllCols().size();
    for (int i = 0; i < partCols.size(); i++) {
      if (first) {
        first = false;
      } else {
        sqlGenerator.append(",");
      }
      appendUpdateColumn(updateBlock, sqlGenerator, insertValues, setColExprs,
          partCols.get(i).getName(), updateBlock.getTargetTable().getCols().size() + i + columnOffset);
    }
  }
}
