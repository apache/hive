/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.predicate;

import java.nio.ByteBuffer;

import org.apache.hadoop.hive.accumulo.predicate.compare.CompareOp;
import org.apache.hadoop.hive.accumulo.predicate.compare.DoubleCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.IntCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.LongCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.PrimitiveComparison;
import org.apache.hadoop.hive.accumulo.predicate.compare.StringCompare;
import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

/**
 * For use in IteratorSetting construction.
 *
 * encapsulates a constant byte [], PrimitiveCompare instance, and CompareOp instance.
 */
public class PushdownTuple {
  private static final Logger log = Logger.getLogger(PushdownTuple.class);

  private byte[] constVal;
  private PrimitiveComparison pCompare;
  private CompareOp cOpt;

  public PushdownTuple(IndexSearchCondition sc, PrimitiveComparison pCompare, CompareOp cOpt)
      throws SerDeException {
    ExprNodeConstantEvaluator eval = new ExprNodeConstantEvaluator(sc.getConstantDesc());

    try {
      this.pCompare = pCompare;
      this.cOpt = cOpt;
      Writable writable = (Writable) eval.evaluate(null);
      constVal = getConstantAsBytes(writable);
    } catch (ClassCastException cce) {
      log.info(StringUtils.stringifyException(cce));
      throw new SerDeException(" Column type mismatch in where clause "
          + sc.getComparisonExpr().getExprString() + " found type "
          + sc.getConstantDesc().getTypeString() + " instead of "
          + sc.getColumnDesc().getTypeString());
    } catch (HiveException e) {
      throw new SerDeException(e);
    }
  }

  public byte[] getConstVal() {
    return constVal;
  }

  public PrimitiveComparison getpCompare() {
    return pCompare;
  }

  public CompareOp getcOpt() {
    return cOpt;
  }

  /**
   *
   * @return byte [] value from writable.
   * @throws SerDeException
   */
  public byte[] getConstantAsBytes(Writable writable) throws SerDeException {
    if (pCompare instanceof StringCompare) {
      return writable.toString().getBytes();
    } else if (pCompare instanceof DoubleCompare) {
      byte[] bts = new byte[8];
      double val = ((DoubleWritable) writable).get();
      ByteBuffer.wrap(bts).putDouble(val);
      return bts;
    } else if (pCompare instanceof IntCompare) {
      byte[] bts = new byte[4];
      int val = ((IntWritable) writable).get();
      ByteBuffer.wrap(bts).putInt(val);
      return bts;
    } else if (pCompare instanceof LongCompare) {
      byte[] bts = new byte[8];
      long val = ((LongWritable) writable).get();
      ByteBuffer.wrap(bts).putLong(val);
      return bts;
    } else {
      throw new SerDeException("Unsupported primitive category: " + pCompare.getClass().getName());
    }
  }

}
