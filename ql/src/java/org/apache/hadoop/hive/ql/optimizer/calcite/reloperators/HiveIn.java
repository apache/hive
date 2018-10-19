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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class HiveIn extends SqlSpecialOperator {

  public static final SqlSpecialOperator INSTANCE =
          new HiveIn();

  private HiveIn() {
    super(
        "IN",
        SqlKind.IN,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        null);
  }

  @Override
  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    List<SqlNode> opList = call.getOperandList();
    assert (opList.size() >= 1);

    SqlNode sqlNode = opList.get(0);
    sqlNode.unparse(writer, leftPrec, getLeftPrec());
    writer.sep("IN");
    Frame frame = writer.startList(FrameTypeEnum.SETOP, "(", ")");
    for (SqlNode op : opList.subList(1, opList.size())) {
      writer.sep(",");
      op.unparse(writer, 0, 0);
    }
    writer.endList(frame);

  }
}
