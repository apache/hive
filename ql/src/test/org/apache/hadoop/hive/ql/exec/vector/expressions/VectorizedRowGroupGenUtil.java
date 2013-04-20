package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class VectorizedRowGroupGenUtil {

  public static VectorizedRowBatch getVectorizedRowBatch(int size, int numCol, int seed) {
    VectorizedRowBatch vrg = new VectorizedRowBatch(numCol, size);
    for (int j = 0; j < numCol; j++) {
      LongColumnVector lcv = new LongColumnVector(size);
      for (int i = 0; i < size; i++) {
        lcv.vector[i] = i * seed * j;
      }
      vrg.cols[j] = lcv;
    }
    vrg.size = size;
    return vrg;
  }
}
