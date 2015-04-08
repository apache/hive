package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;

public class HiveGroupingID extends SqlAggFunction {

  public static final SqlAggFunction INSTANCE =
          new HiveGroupingID();

  private HiveGroupingID() {
    super(VirtualColumn.GROUPINGID.getName(),
            SqlKind.OTHER,
            ReturnTypes.INTEGER,
            InferTypes.BOOLEAN,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

}
