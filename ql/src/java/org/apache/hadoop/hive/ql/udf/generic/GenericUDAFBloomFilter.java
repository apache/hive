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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.io.NonSyncByteArrayInputStream;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedUDAFs;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.BloomKFilter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Generic UDF to generate Bloom Filter
 */
public class GenericUDAFBloomFilter implements GenericUDAFResolver2 {

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
    return new GenericUDAFBloomFilterEvaluator();
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    return new GenericUDAFBloomFilterEvaluator();
  }

  /**
   * GenericUDAFBloomFilterEvaluator - Evaluator class for BloomFilter
   */
  @VectorizedUDAFs({
    VectorUDAFBloomFilter.class,
    VectorUDAFBloomFilterMerge.class})
  public static class GenericUDAFBloomFilterEvaluator extends GenericUDAFEvaluator {
    // Source operator to get the number of entries
    private SelectOperator sourceOperator;
    private long hintEntries = -1;
    private long maxEntries = 0;
    private long minEntries = 0;
    private float factor = 1;

    // ObjectInspector for input data.
    private PrimitiveObjectInspector inputOI;

    // Bloom filter rest
    private final ByteArrayOutputStream result = new ByteArrayOutputStream();

    private transient byte[] scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // Initialize input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
      } else {
        // Do nothing for other modes
      }

      // Output will be same in both partial or full aggregation modes.
      // It will be a BloomFilter in ByteWritable
      return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }

    @Override
    public int estimate() {
      long entries = Math.min(getExpectedEntries(), maxEntries);
      long numBits = (long) (-entries * Math.log(BloomKFilter.DEFAULT_FPP) / (Math.log(2) * Math.log(2)));
      int nLongs = (int) Math.ceil((double) numBits / (double) Long.SIZE);
      // additional bits to pad long array to block size
      int padLongs = 8 - nLongs % 8;
      return (nLongs + padLongs) * Long.SIZE / 8;
    }

    /**
     * Class for storing the BloomFilter
     */
    @AggregationType(estimable = true)
    static class BloomFilterBuf extends AbstractAggregationBuffer {
      BloomKFilter bloomFilter;

      public BloomFilterBuf(long expectedEntries, long maxEntries) {
        if (expectedEntries > maxEntries) {
          bloomFilter = new BloomKFilter(maxEntries);
        } else {
          bloomFilter = new BloomKFilter(expectedEntries);
        }
      }

      @Override
      public int estimate() {
        return (int) bloomFilter.sizeInBytes();
      }
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((BloomFilterBuf)agg).bloomFilter.reset();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      long expectedEntries = getExpectedEntries();
      if (expectedEntries < 0) {
        throw new IllegalStateException("BloomFilter expectedEntries not initialized");
      }

      BloomFilterBuf buf = new BloomFilterBuf(expectedEntries, maxEntries);
      reset(buf);
      return buf;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      if (parameters == null || parameters[0] == null) {
        // 2nd condition occurs when the input has 0 rows (possible due to
        // filtering, joins etc).
        return;
      }

      BloomKFilter bf = ((BloomFilterBuf)agg).bloomFilter;

      // Add the expression into the BloomFilter
      switch (inputOI.getPrimitiveCategory()) {
        case BOOLEAN:
          boolean vBoolean = ((BooleanObjectInspector)inputOI).get(parameters[0]);
          bf.addLong(vBoolean ? 1 : 0);
          break;
        case BYTE:
          byte vByte = ((ByteObjectInspector)inputOI).get(parameters[0]);
          bf.addLong(vByte);
          break;
        case SHORT:
          short vShort = ((ShortObjectInspector)inputOI).get(parameters[0]);
          bf.addLong(vShort);
          break;
        case INT:
          int vInt = ((IntObjectInspector)inputOI).get(parameters[0]);
          bf.addLong(vInt);
          break;
        case LONG:
          long vLong = ((LongObjectInspector)inputOI).get(parameters[0]);
          bf.addLong(vLong);
          break;
        case FLOAT:
          float vFloat = ((FloatObjectInspector)inputOI).get(parameters[0]);
          bf.addDouble(vFloat);
          break;
        case DOUBLE:
          double vDouble = ((DoubleObjectInspector)inputOI).get(parameters[0]);
          bf.addDouble(vDouble);
          break;
        case DECIMAL:
          HiveDecimalWritable vDecimal = ((HiveDecimalObjectInspector)inputOI).
                  getPrimitiveWritableObject(parameters[0]);
          int startIdx = vDecimal.toBytes(scratchBuffer);
          bf.addBytes(scratchBuffer, startIdx, scratchBuffer.length - startIdx);
          break;
        case DATE:
          DateWritableV2 vDate = ((DateObjectInspector)inputOI).
                  getPrimitiveWritableObject(parameters[0]);
          bf.addLong(vDate.getDays());
          break;
        case TIMESTAMP:
          Timestamp vTimeStamp = ((TimestampObjectInspector)inputOI).
                  getPrimitiveJavaObject(parameters[0]);
          bf.addLong(vTimeStamp.toEpochMilli());
          break;
        case CHAR:
          Text vChar = ((HiveCharObjectInspector)inputOI).
                  getPrimitiveWritableObject(parameters[0]).getStrippedValue();
          bf.addBytes(vChar.getBytes(), 0, vChar.getLength());
          break;
        case VARCHAR:
          Text vVarChar = ((HiveVarcharObjectInspector)inputOI).
                  getPrimitiveWritableObject(parameters[0]).getTextValue();
          bf.addBytes(vVarChar.getBytes(), 0, vVarChar.getLength());
          break;
        case STRING:
          Text vString = ((StringObjectInspector)inputOI).
                  getPrimitiveWritableObject(parameters[0]);
          bf.addBytes(vString.getBytes(), 0, vString.getLength());
          break;
        case BINARY:
          BytesWritable vBytes = ((BinaryObjectInspector)inputOI).
                  getPrimitiveWritableObject(parameters[0]);
          bf.addBytes(vBytes.getBytes(), 0, vBytes.getLength());
          break;
          default:
            throw new UDFArgumentTypeException(0,
                    "Bad primitive category " + inputOI.getPrimitiveCategory());
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial == null) {
        return;
      }

      BytesWritable bytes = (BytesWritable) partial;
      ByteArrayInputStream in = new NonSyncByteArrayInputStream(bytes.getBytes());
      // Deserialize the bloom filter
      try {
        BloomKFilter bf = BloomKFilter.deserialize(in);
        ((BloomFilterBuf)agg).bloomFilter.merge(bf);
      } catch (IOException e) {
        throw new HiveException(e);
      } finally {
        IOUtils.closeStream(in);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      result.reset();
      try {
        BloomKFilter.serialize(result, ((BloomFilterBuf)agg).bloomFilter);
      } catch (IOException e) {
        throw new HiveException(e);
      } finally {
        IOUtils.closeStream(result);
      }
      return new BytesWritable(result.toByteArray());
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return terminate(agg);
    }

    public long getExpectedEntries() {
      // If hint is provided use that size.
      if (hintEntries > 0 )
        return hintEntries;

      long expectedEntries = -1;
      if (sourceOperator != null && sourceOperator.getStatistics() != null) {
        Statistics stats = sourceOperator.getStatistics();
        expectedEntries = stats.getNumRows();

        // Use NumDistinctValues if possible
        switch (stats.getColumnStatsState()) {
          case COMPLETE:
          case PARTIAL:
            // There should only be column in sourceOperator
            List<ColStatistics> colStats = stats.getColumnStats();
            ExprNodeColumnDesc colExpr = ExprNodeDescUtils.getColumnExpr(
                sourceOperator.getConf().getColList().get(0));
            if (colExpr != null
                && stats.getColumnStatisticsFromColName(colExpr.getColumn()) != null) {
              long ndv = stats.getColumnStatisticsFromColName(colExpr.getColumn()).getCountDistint();
              if (ndv > 0) {
                expectedEntries = ndv;
              }
            }
            break;
          default:
            break;
        }
      }

      // Update expectedEntries based on factor and minEntries configurations
      expectedEntries = (long) (expectedEntries * factor);
      expectedEntries = expectedEntries > minEntries ? expectedEntries : minEntries;
      return expectedEntries;
    }

    public Operator<?> getSourceOperator() {
      return sourceOperator;
    }

    public void setSourceOperator(SelectOperator sourceOperator) {
      this.sourceOperator = sourceOperator;
    }

    public void setHintEntries(long hintEntries) {
      this.hintEntries = hintEntries;
    }

    public boolean hasHintEntries() {
      return hintEntries != -1;
    }

    public void setMaxEntries(long maxEntries) {
      this.maxEntries = maxEntries;
    }

    public void setMinEntries(long minEntries) {
      this.minEntries = minEntries;
    }

    public long getMinEntries() {
      return minEntries;
    }

    public void setFactor(float factor) {
      this.factor = factor;
    }

    public float getFactor() {
      return factor;
    }

    @Override
    public String getExprString() {
      return "expectedEntries=" + getExpectedEntries();
    }
  }
}
