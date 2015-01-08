package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class HiveGroupingID extends SqlInternalOperator {

  public static final SqlInternalOperator GROUPING__ID =
          new HiveGroupingID();

  private HiveGroupingID() {
    super("$GROUPING__ID",
            SqlKind.OTHER,
            0,
            false,
            ReturnTypes.BIGINT,
            InferTypes.BOOLEAN,
            OperandTypes.ONE_OR_MORE);
  }

}

