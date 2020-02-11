/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class HiveConcat extends SqlSpecialOperator {
  public static final SqlSpecialOperator INSTANCE = new HiveConcat();

  private HiveConcat() {
    super("||", SqlKind.OTHER_FUNCTION, 30, true, ReturnTypes.VARCHAR_2000,
        InferTypes.RETURN_TYPE, null
    );
  }

  @Override
  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    List<SqlNode> opList = call.getOperandList();
    assert (opList.size() >= 1);
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
    SqlNode sqlNode = opList.get(0);
    sqlNode.unparse(writer, leftPrec, getLeftPrec());
    for (SqlNode op : opList.subList(1, opList.size() - 1)) {
      writer.setNeedWhitespace(true);
      writer.sep("||");
      writer.setNeedWhitespace(true);
      op.unparse(writer, 0, 0);
    }
    sqlNode = opList.get(opList.size() - 1);
    writer.setNeedWhitespace(true);
    writer.sep("||");
    writer.setNeedWhitespace(true);
    sqlNode.unparse(writer, getRightPrec(), rightPrec);
    writer.endList(frame);
  }

}
