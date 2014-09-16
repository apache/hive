/**
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


package org.apache.hadoop.hive.llap.api;

import java.io.IOException;

import org.apache.hadoop.hive.common.type.Decimal128;

public interface Vector {
  public static enum Type {
    LONG(0), DOUBLE(1), DECIMAL(2), BINARY(3);
    private byte value;
    private Type(int value) {
      assert value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE;
      this.value = (byte)value;
    }
    public byte value() {
      return value;
    }
  }

  /**
   * @return Number of columns in this vector.
   */
  public int getNumberOfColumns();

  /**
   * @return Number of rows in this vector.
   */
  public int getNumberOfRows();

  /**
   * Prepare vector to read values at a particular offset, from particular column.
   * @param colIx Column index.
   * @param rowOffset Row offset inside vector.
   */
  public ColumnReader next(int colIx, int rowCount);

  public interface ColumnReader {
    /**
     * @return Whether there's a run on a single value in a row range for a column.
     */
    public boolean isSameValue();

    /**
     * @return Whether there are any nulls in a row range for a column.
     */
    public boolean hasNulls();

    /**
     * @return Long value from a specific cell.
     */
    public long getLong();

    /**
     * Extracts long values from a row range for a column. Can set any value for nulls.
     * @param dest Destination array.
     * @param isNulls
     * @param offset Offset to write to dest from.
     * @param rowCount Row (cell) count to extract.
     */
    public void copyLongs(long[] dest, boolean[] isNulls, int offset) throws IOException;

    public void copyDoubles(double[] dest, boolean[] isNull, int offset) throws IOException;

    /**
     * @return Double value from a specific cell.
     */
    public double getDouble();

    /**
     * @return Decimal value from a specific cell.
     */
    public Decimal128 getDecimal();

    public void copyDecimals(Decimal128[] dest, boolean[] isNull, int offset) throws IOException;

    /**
     * @return byte[] value from a specific cell.
     */
    public byte[] getBytes();

    /**
     * Extracts byte[] values from a row range for a column. Can set any value for nulls.
     * @param dest Destination array of base arrays for values.
     * @param destStarts Destination array start offsets (to define slices of base arrays).
     * @param destLengths Destination array value lengths (to define slices of base arrays).
     * @param offset Offset to write to dest from.
     * @param rowCount Row (cell) count to extract.
     */
    public void copyBytes(byte[][] dest, int[] destStarts, int[] destLengths,
        boolean[] isNull, int offset) throws IOException;
  }
}
