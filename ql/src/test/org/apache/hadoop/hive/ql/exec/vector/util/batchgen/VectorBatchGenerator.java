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

package org.apache.hadoop.hive.ql.exec.vector.util.batchgen;

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType.GenerateCategory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import com.google.common.base.Preconditions;

public class VectorBatchGenerator {

  public static class GenerateType {

    // UNDONE: Missing date/time interval data types
    public enum GenerateCategory {
      BOOLEAN("boolean", true),
      BYTE("tinyint", true),
      SHORT("smallint", true),
      INT("int", true),
      LONG("bigint", true),
      FLOAT("float", true),
      DOUBLE("double", true),
      STRING("string", true),
      DATE("date", true),
      TIMESTAMP("timestamp", true),
      BINARY("binary", true),
      DECIMAL("decimal", true),
      VARCHAR("varchar", true),
      CHAR("char", true),
      LIST("array", false),
      MAP("map", false),
      STRUCT("struct", false),
      UNION("uniontype", false);

      GenerateCategory(String name, boolean isPrimitive) {
        this.name = name;
        this.isPrimitive = isPrimitive;
      }

      final boolean isPrimitive;
      final String name;

      public boolean isPrimitive() {
        return isPrimitive;
      }

      public String getName() {
        return name;
      }

      public static GenerateCategory generateCategoryFromPrimitiveCategory(PrimitiveCategory primitiveCategory) {
        switch (primitiveCategory) {
        case BOOLEAN:
          return GenerateCategory.BOOLEAN;
        case BYTE:
          return GenerateCategory.BYTE;
        case SHORT:
          return GenerateCategory.SHORT;
        case INT:
          return GenerateCategory.INT;
        case LONG:
          return GenerateCategory.LONG;
        case FLOAT:
          return GenerateCategory.FLOAT;
        case DOUBLE:
          return GenerateCategory.DOUBLE;
        case STRING:
          return GenerateCategory.STRING;
        case DATE:
          return GenerateCategory.DATE;
        case TIMESTAMP:
          return GenerateCategory.TIMESTAMP;
        case BINARY:
          return GenerateCategory.BINARY;
        case DECIMAL:
          return GenerateCategory.DECIMAL;
        case VARCHAR:
          return GenerateCategory.VARCHAR;
        case CHAR:
          return GenerateCategory.CHAR;
        default:
          return null;
        }
      }
    }

    private GenerateCategory category;
    private boolean allowNulls;

    public GenerateType(GenerateCategory category) {
      this.category = category;
    }

    public GenerateType(GenerateCategory category, boolean allowNulls) {
      this.category = category;
      this.allowNulls = allowNulls;
    }

    public GenerateCategory getCategory() {
      return category;
    }

    public boolean getAllowNulls() {
      return allowNulls;
    }

    /*
     * BOOLEAN .. LONG: Min and max.
     */
    private long integerMin;
    private long integerMax;

    /*
     * FLOAT: Min and max.
     */
    private float floatMin;
    private float floatMax;

    /*
     * DOUBLE: Min and max.
     */
    private double doubleMin;
    private double doubleMax;

    /*
     * STRING:
     *   Range, values, empty strings.
     */

    /*
     * CHAR: strategic blanks, string length beyond max
     */

    /*
     * VARCHAR: string length beyond max
     */
  }

  private VectorColumnGroupGenerator[] columnGroups;
  private boolean[] isGenerateSeries;

  public VectorBatchGenerator(GenerateType[] generateTypes) {
    final int size = generateTypes.length;
    columnGroups = new VectorColumnGroupGenerator[size];
    for (int i = 0; i < size; i++) {
      columnGroups[i] = new VectorColumnGroupGenerator(i, generateTypes[i]);
    }
    isGenerateSeries = new boolean[size];
    // UNDONE: For now, all...
    Arrays.fill(isGenerateSeries, true);
  }

  public VectorBatchGenerator(VectorColumnGroupGenerator[] columnGroups) {
    this.columnGroups = columnGroups;
  }

  public void assignColumnVectors(VectorizedRowBatch batch, int columnNum,
      VectorColumnGroupGenerator columnGroup) {
    // UNDONE: Multiple types...
    GenerateType[] generateTypes = columnGroup.generateTypes();
    GenerateType generateType = generateTypes[0];
    ColumnVector colVector;
    switch (generateType.getCategory()) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      colVector = new LongColumnVector();
      break;

    case DATE:
      colVector = new DateColumnVector();
      break;

    case FLOAT:
    case DOUBLE:
      colVector = new DoubleColumnVector();
      break;

    case STRING:
    case CHAR:
    case VARCHAR:
    case BINARY:
      colVector = new BytesColumnVector();
      break;

    case TIMESTAMP:
      colVector = new TimestampColumnVector();
      break;

    case DECIMAL:
      colVector = new DecimalColumnVector(38, 18);
      break;

    // UNDONE

    case LIST:
    case MAP:
    case STRUCT:
    case UNION:
    default:
      throw new RuntimeException("Unsupported catagory " + generateType.getCategory());
    }
    colVector.init();
    batch.cols[columnNum] = colVector;
  }

  public VectorizedRowBatch createBatch() {
    final int size = columnGroups.length;
    VectorizedRowBatch batch = new VectorizedRowBatch(size);
    for (int i = 0; i < size; i++) {
      assignColumnVectors(batch, i, columnGroups[i]);
    }
    return batch;
  }

  public void generateBatch(VectorizedRowBatch batch, Random random,
      int size) {

    // Clear value arrays.
    for (int c = 0; c < columnGroups.length; c++) {
      columnGroups[c].clearColumnValueArrays();
    }

    // Generate row values.
    int i = 0;
    while (true) {
      for (int c = 0; c < columnGroups.length; c++) {
        columnGroups[c].generateRowValues(i, random);
      }
      if (i + 1 >= size) {
        break;
      }

      // Null out some row column entries.
      // UNDONE

      // Consider generating a column group equal value series?
      if (i < size - 1) {
        for (int c = 0; c < columnGroups.length; c++) {
          if (isGenerateSeries[c]) {
            int seriesCount = getSeriesCount(random);
            if (seriesCount == 1) {
              continue;
            }
            seriesCount = Math.min(seriesCount, size - i);
            Preconditions.checkState(seriesCount > 1);

            // Fill values down for equal value series.
            VectorColumnGroupGenerator columnGroup = columnGroups[c];
            columnGroup.fillDownRowValues(i, seriesCount, random);

            // For all the other column groups, generate new values down.
            for (int other = 0; other < columnGroups.length; other++) {
              if (other != c) {
                VectorColumnGroupGenerator otherColumnGroup = columnGroups[other];
                otherColumnGroup.generateDownRowValues(i, seriesCount, random);

                // Also, null down.
                // UNDONE
              }
            }

            // Fill down null flags.
            // UNDONE

            i += (seriesCount - 1);
            break;
          }
        }
      }
      // Recheck.
      i++;
      if (i >= size) {
        break;
      }
    }

    // Optionally, do some filtering of rows...
    // UNDONE

    // From the value arrays and our isRepeated, selected, isNull arrays, generate the batch!
    for (int c = 0; c < columnGroups.length; c++) {
      VectorColumnGroupGenerator columnGroup = columnGroups[c];

      // UNDONE: Provide isRepeated, selected, isNull
      columnGroup.populateBatch(batch, size, false);
    }

    batch.size = size;
  }

  private int getSeriesCount(Random random) {
    // UNDONE: For now...
    if (random.nextBoolean()) {
      return 1;
    } else {
      return 1 + random.nextInt(10);
    }
  }
}