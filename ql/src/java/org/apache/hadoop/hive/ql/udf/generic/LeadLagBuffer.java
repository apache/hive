package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;

interface LeadLagBuffer extends AggregationBuffer {
  void initialize(int leadAmt);

  void addRow(Object leadExprValue, Object defaultValue);

  Object terminate();

}