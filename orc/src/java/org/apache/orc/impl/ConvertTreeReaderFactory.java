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
package org.apache.orc.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.EnumMap;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.util.TimestampUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;

/**
 * Convert ORC tree readers.
 */
public class ConvertTreeReaderFactory extends TreeReaderFactory {

  /**
   * Override methods like checkEncoding to pass-thru to the convert TreeReader.
   */
  public static class ConvertTreeReader extends TreeReader {

    private TreeReader convertTreeReader;

    ConvertTreeReader(int columnId) throws IOException {
      super(columnId);
    }

    // The ordering of types here is used to determine which numeric types
    // are common/convertible to one another. Probably better to rely on the
    // ordering explicitly defined here than to assume that the enum values
    // that were arbitrarily assigned in PrimitiveCategory work for our purposes.
    private static EnumMap<TypeDescription.Category, Integer> numericTypes =
        new EnumMap<>(TypeDescription.Category.class);

    static {
      registerNumericType(TypeDescription.Category.BOOLEAN, 1);
      registerNumericType(TypeDescription.Category.BYTE, 2);
      registerNumericType(TypeDescription.Category.SHORT, 3);
      registerNumericType(TypeDescription.Category.INT, 4);
      registerNumericType(TypeDescription.Category.LONG, 5);
      registerNumericType(TypeDescription.Category.FLOAT, 6);
      registerNumericType(TypeDescription.Category.DOUBLE, 7);
      registerNumericType(TypeDescription.Category.DECIMAL, 8);
    }

    private static void registerNumericType(TypeDescription.Category kind, int level) {
      numericTypes.put(kind, level);
    }

    protected void setConvertTreeReader(TreeReader convertTreeReader) {
      this.convertTreeReader = convertTreeReader;
    }

    protected TreeReader getStringGroupTreeReader(int columnId,
        TypeDescription fileType) throws IOException {
      switch (fileType.getCategory()) {
      case STRING:
        return new StringTreeReader(columnId);
      case CHAR:
        return new CharTreeReader(columnId, fileType.getMaxLength());
      case VARCHAR:
        return new VarcharTreeReader(columnId, fileType.getMaxLength());
      default:
        throw new RuntimeException("Unexpected type kind " + fileType.getCategory().name());
      }
    }

    protected void assignStringGroupVectorEntry(BytesColumnVector bytesColVector,
        int elementNum, TypeDescription readerType, byte[] bytes) {
      assignStringGroupVectorEntry(bytesColVector,
          elementNum, readerType, bytes, 0, bytes.length);
    }

    /*
     * Assign a BytesColumnVector entry when we have a byte array, start, and
     * length for the string group which can be (STRING, CHAR, VARCHAR).
     */
    protected void assignStringGroupVectorEntry(BytesColumnVector bytesColVector,
        int elementNum, TypeDescription readerType, byte[] bytes, int start, int length) {
      switch (readerType.getCategory()) {
      case STRING:
        bytesColVector.setVal(elementNum, bytes, start, length);
        break;
      case CHAR:
        {
          int adjustedDownLen =
              StringExpr.rightTrimAndTruncate(bytes, start, length, readerType.getMaxLength());
          bytesColVector.setVal(elementNum, bytes, start, adjustedDownLen);
        }
        break;
      case VARCHAR:
        {
          int adjustedDownLen =
              StringExpr.truncate(bytes, start, length, readerType.getMaxLength());
          bytesColVector.setVal(elementNum, bytes, start, adjustedDownLen);
        }
        break;
      default:
        throw new RuntimeException("Unexpected type kind " + readerType.getCategory().name());
      }
    }

    protected void convertStringGroupVectorElement(BytesColumnVector bytesColVector,
        int elementNum, TypeDescription readerType) {
      switch (readerType.getCategory()) {
      case STRING:
        // No conversion needed.
        break;
      case CHAR:
        {
          int length = bytesColVector.length[elementNum];
          int adjustedDownLen = StringExpr
            .rightTrimAndTruncate(bytesColVector.vector[elementNum],
                bytesColVector.start[elementNum], length,
                readerType.getMaxLength());
          if (adjustedDownLen < length) {
            bytesColVector.length[elementNum] = adjustedDownLen;
          }
        }
        break;
      case VARCHAR:
        {
          int length = bytesColVector.length[elementNum];
          int adjustedDownLen = StringExpr
            .truncate(bytesColVector.vector[elementNum],
                bytesColVector.start[elementNum], length,
                readerType.getMaxLength());
          if (adjustedDownLen < length) {
            bytesColVector.length[elementNum] = adjustedDownLen;
          }
        }
        break;
      default:
        throw new RuntimeException("Unexpected type kind " + readerType.getCategory().name());
      }
    }

    private boolean isParseError;

    /*
     * We do this because we want the various parse methods return a primitive.
     *
     * @return true if there was a parse error in the last call to
     * parseLongFromString, etc.
     */
    protected boolean getIsParseError() {
      return isParseError;
    }

    protected long parseLongFromString(String string) {
      try {
        long longValue = Long.parseLong(string);
        isParseError = false;
        return longValue;
      } catch (NumberFormatException e) {
        isParseError = true;
        return 0;
      }
    }

    protected float parseFloatFromString(String string) {
      try {
        float floatValue = Float.parseFloat(string);
        isParseError = false;
        return floatValue;
      } catch (NumberFormatException e) {
        isParseError = true;
        return Float.NaN;
      }
    }

    protected double parseDoubleFromString(String string) {
      try {
        double value = Double.parseDouble(string);
        isParseError = false;
        return value;
      } catch (NumberFormatException e) {
        isParseError = true;
        return Double.NaN;
      }
    }

    /**
     * @param string
     * @return the HiveDecimal parsed, or null if there was a parse error.
     */
    protected HiveDecimal parseDecimalFromString(String string) {
      try {
        HiveDecimal value = HiveDecimal.create(string);
        return value;
      } catch (NumberFormatException e) {
        return null;
      }
    }

    /**
     * @param string
     * @return the Timestamp parsed, or null if there was a parse error.
     */
    protected Timestamp parseTimestampFromString(String string) {
      try {
        Timestamp value = Timestamp.valueOf(string);
        return value;
      } catch (IllegalArgumentException e) {
        return null;
      }
    }

    /**
     * @param string
     * @return the Date parsed, or null if there was a parse error.
     */
    protected Date parseDateFromString(String string) {
      try {
        Date value = Date.valueOf(string);
        return value;
      } catch (IllegalArgumentException e) {
        return null;
      }
    }

    protected String stringFromBytesColumnVectorEntry(
        BytesColumnVector bytesColVector, int elementNum) {
      String string;

      string = new String(
          bytesColVector.vector[elementNum],
          bytesColVector.start[elementNum], bytesColVector.length[elementNum],
          StandardCharsets.UTF_8);

      return string;
    }

    private static final double MIN_LONG_AS_DOUBLE = -0x1p63;
    /*
     * We cannot store Long.MAX_VALUE as a double without losing precision. Instead, we store
     * Long.MAX_VALUE + 1 == -Long.MIN_VALUE, and then offset all comparisons by 1.
     */
    private static final double MAX_LONG_AS_DOUBLE_PLUS_ONE = 0x1p63;

    public boolean doubleCanFitInLong(double doubleValue) {

      // Borrowed from Guava DoubleMath.roundToLong except do not want dependency on Guava and we
      // don't want to catch an exception.

      return ((MIN_LONG_AS_DOUBLE - doubleValue < 1.0) &&
              (doubleValue < MAX_LONG_AS_DOUBLE_PLUS_ONE));
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      // Pass-thru.
      convertTreeReader.checkEncoding(encoding);
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      // Pass-thru.
      convertTreeReader.startStripe(streams, stripeFooter);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
     // Pass-thru.
      convertTreeReader.seek(index);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      // Pass-thru.
      convertTreeReader.seek(index);
    }

    @Override
    void skipRows(long items) throws IOException {
      // Pass-thru.
      convertTreeReader.skipRows(items);
    }

    /**
     * Override this to use convertVector.
     * Source and result are member variables in the subclass with the right
     * type.
     * @param elementNum
     * @throws IOException
     */
    // Override this to use convertVector.
    public void setConvertVectorElement(int elementNum) throws IOException {
      throw new RuntimeException("Expected this method to be overriden");
    }

    // Common code used by the conversion.
    public void convertVector(ColumnVector fromColVector,
        ColumnVector resultColVector, final int batchSize) throws IOException {

      resultColVector.reset();
      if (fromColVector.isRepeating) {
        resultColVector.isRepeating = true;
        if (fromColVector.noNulls || !fromColVector.isNull[0]) {
          setConvertVectorElement(0);
        } else {
          resultColVector.noNulls = false;
          resultColVector.isNull[0] = true;
        }
      } else if (fromColVector.noNulls){
        for (int i = 0; i < batchSize; i++) {
          setConvertVectorElement(i);
        }
      } else {
        for (int i = 0; i < batchSize; i++) {
          if (!fromColVector.isNull[i]) {
            setConvertVectorElement(i);
          } else {
            resultColVector.noNulls = false;
            resultColVector.isNull[i] = true;
          }
        }
      }
    }

    public void downCastAnyInteger(LongColumnVector longColVector, int elementNum,
        TypeDescription readerType) {
      downCastAnyInteger(longColVector, elementNum, longColVector.vector[elementNum], readerType);
    }

    public void downCastAnyInteger(LongColumnVector longColVector, int elementNum, long inputLong,
        TypeDescription readerType) {
      long[] vector = longColVector.vector;
      long outputLong;
      Category readerCategory = readerType.getCategory();
      switch (readerCategory) {
      case BOOLEAN:
        // No data loss for boolean.
        vector[elementNum] = inputLong == 0 ? 0 : 1;
        return;
      case BYTE:
        outputLong = (byte) inputLong;
        break;
      case SHORT:
        outputLong = (short) inputLong;
        break;
      case INT:
        outputLong = (int) inputLong;
        break;
      case LONG:
        // No data loss for long.
        vector[elementNum] = inputLong;
        return;
      default:
        throw new RuntimeException("Unexpected type kind " + readerCategory.name());
      }

      if (outputLong != inputLong) {
        // Data loss.
        longColVector.isNull[elementNum] = true;
        longColVector.noNulls = false;
      } else {
        vector[elementNum] = outputLong;
      }
    }

    protected boolean integerDownCastNeeded(TypeDescription fileType, TypeDescription readerType) {
      Integer fileLevel = numericTypes.get(fileType.getCategory());
      Integer schemaLevel = numericTypes.get(readerType.getCategory());
      return (schemaLevel.intValue() < fileLevel.intValue());
    }
  }

  public static class AnyIntegerTreeReader extends ConvertTreeReader {

    private TypeDescription.Category fileTypeCategory;
    private TreeReader anyIntegerTreeReader;

    private long longValue;

    AnyIntegerTreeReader(int columnId, TypeDescription fileType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      this.fileTypeCategory = fileType.getCategory();
      switch (fileTypeCategory) {
      case BOOLEAN:
        anyIntegerTreeReader = new BooleanTreeReader(columnId);
        break;
      case BYTE:
        anyIntegerTreeReader = new ByteTreeReader(columnId);
        break;
      case SHORT:
        anyIntegerTreeReader = new ShortTreeReader(columnId);
        break;
      case INT:
        anyIntegerTreeReader = new IntTreeReader(columnId);
        break;
      case LONG:
        anyIntegerTreeReader = new LongTreeReader(columnId, skipCorrupt);
        break;
      default:
        throw new RuntimeException("Unexpected type kind " + fileType.getCategory().name());
      }
      setConvertTreeReader(anyIntegerTreeReader);
    }

    protected long getLong() throws IOException {
      return longValue;
    }

    protected String getString(long longValue) {
      if (fileTypeCategory == TypeDescription.Category.BOOLEAN) {
        return longValue == 0 ? "FALSE" : "TRUE";
      } else {
        return Long.toString(longValue);
      }
    }

    protected String getString() {
      return getString(longValue);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      anyIntegerTreeReader.nextVector(previousVector, isNull, batchSize);
    }
  }

  public static class AnyIntegerFromAnyIntegerTreeReader extends ConvertTreeReader {

    private AnyIntegerTreeReader anyIntegerAsLongTreeReader;

    private final TypeDescription readerType;
    private final boolean downCastNeeded;

    AnyIntegerFromAnyIntegerTreeReader(int columnId, TypeDescription fileType, TypeDescription readerType, boolean skipCorrupt) throws IOException {
      super(columnId);
      this.readerType = readerType;
      anyIntegerAsLongTreeReader = new AnyIntegerTreeReader(columnId, fileType, skipCorrupt);
      setConvertTreeReader(anyIntegerAsLongTreeReader);
      downCastNeeded = integerDownCastNeeded(fileType, readerType);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      anyIntegerAsLongTreeReader.nextVector(previousVector, isNull, batchSize);
      LongColumnVector resultColVector = (LongColumnVector) previousVector;
      if (downCastNeeded) {
        if (resultColVector.isRepeating) {
          if (resultColVector.noNulls || !resultColVector.isNull[0]) {
            downCastAnyInteger(resultColVector, 0, readerType);
          } else {
            // Result remains null.
          }
        } else if (resultColVector.noNulls){
          for (int i = 0; i < batchSize; i++) {
            downCastAnyInteger(resultColVector, i, readerType);
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            if (!resultColVector.isNull[i]) {
              downCastAnyInteger(resultColVector, i, readerType);
            } else {
              // Result remains null.
            }
          }
        }
      }
    }
  }

  public static class AnyIntegerFromFloatTreeReader extends ConvertTreeReader {

    private FloatTreeReader floatTreeReader;

    private final TypeDescription readerType;
    private DoubleColumnVector doubleColVector;
    private LongColumnVector longColVector;

    AnyIntegerFromFloatTreeReader(int columnId, TypeDescription readerType)
        throws IOException {
      super(columnId);
      this.readerType = readerType;
      floatTreeReader = new FloatTreeReader(columnId);
      setConvertTreeReader(floatTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      double doubleValue = doubleColVector.vector[elementNum];
      if (!doubleCanFitInLong(doubleValue)) {
        longColVector.isNull[elementNum] = true;
        longColVector.noNulls = false;
      } else {
        // UNDONE: Does the overflow check above using double really work here for float?
        float floatValue = (float) doubleValue;
        downCastAnyInteger(longColVector, elementNum, (long) floatValue, readerType);
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector();
        longColVector = (LongColumnVector) previousVector;
      }
      // Read present/isNull stream
      floatTreeReader.nextVector(doubleColVector, isNull, batchSize);

      convertVector(doubleColVector, longColVector, batchSize);
    }
  }

  public static class AnyIntegerFromDoubleTreeReader extends ConvertTreeReader {

    private DoubleTreeReader doubleTreeReader;

    private final TypeDescription readerType;
    private DoubleColumnVector doubleColVector;
    private LongColumnVector longColVector;

    AnyIntegerFromDoubleTreeReader(int columnId, TypeDescription readerType)
        throws IOException {
      super(columnId);
      this.readerType = readerType;
      doubleTreeReader = new DoubleTreeReader(columnId);
      setConvertTreeReader(doubleTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      double doubleValue = doubleColVector.vector[elementNum];
      if (!doubleCanFitInLong(doubleValue)) {
        longColVector.isNull[elementNum] = true;
        longColVector.noNulls = false;
      } else {
        downCastAnyInteger(longColVector, elementNum, (long) doubleValue, readerType);
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector();
        longColVector = (LongColumnVector) previousVector;
      }
      // Read present/isNull stream
      doubleTreeReader.nextVector(doubleColVector, isNull, batchSize);

      convertVector(doubleColVector, longColVector, batchSize);
    }
  }

  public static class AnyIntegerFromDecimalTreeReader extends ConvertTreeReader {

    private DecimalTreeReader decimalTreeReader;

    private final int precision;
    private final int scale;
    private final TypeDescription readerType;
    private DecimalColumnVector decimalColVector;
    private LongColumnVector longColVector;

    AnyIntegerFromDecimalTreeReader(int columnId, TypeDescription fileType,
        TypeDescription readerType) throws IOException {
      super(columnId);
      this.precision = fileType.getPrecision();
      this.scale = fileType.getScale();
      this.readerType = readerType;
      decimalTreeReader = new DecimalTreeReader(columnId, precision, scale);
      setConvertTreeReader(decimalTreeReader);
    }

    private static HiveDecimal DECIMAL_MAX_LONG = HiveDecimal.create(Long.MAX_VALUE);
    private static HiveDecimal DECIMAL_MIN_LONG = HiveDecimal.create(Long.MIN_VALUE);

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      HiveDecimal decimalValue = decimalColVector.vector[elementNum].getHiveDecimal();
      if (decimalValue.compareTo(DECIMAL_MAX_LONG) > 0 ||
          decimalValue.compareTo(DECIMAL_MIN_LONG) < 0) {
        longColVector.isNull[elementNum] = true;
        longColVector.noNulls = false;
      } else {
        downCastAnyInteger(longColVector, elementNum, decimalValue.longValue(), readerType);
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (decimalColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        decimalColVector = new DecimalColumnVector(precision, scale);
        longColVector = (LongColumnVector) previousVector;
      }
      // Read present/isNull stream
      decimalTreeReader.nextVector(decimalColVector, isNull, batchSize);

      convertVector(decimalColVector, longColVector, batchSize);
    }
  }

  public static class AnyIntegerFromStringGroupTreeReader extends ConvertTreeReader {

    private TreeReader stringGroupTreeReader;

    private final TypeDescription readerType;
    private BytesColumnVector bytesColVector;
    private LongColumnVector longColVector;

    AnyIntegerFromStringGroupTreeReader(int columnId, TypeDescription fileType,
        TypeDescription readerType) throws IOException {
      super(columnId);
      this.readerType = readerType;
      stringGroupTreeReader = getStringGroupTreeReader(columnId, fileType);
      setConvertTreeReader(stringGroupTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String string = stringFromBytesColumnVectorEntry(bytesColVector, elementNum);
      long longValue = parseLongFromString(string);
      if (!getIsParseError()) {
        downCastAnyInteger(longColVector, elementNum, longValue, readerType);
      } else {
        longColVector.noNulls = false;
        longColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (bytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        bytesColVector = new BytesColumnVector();
        longColVector = (LongColumnVector) previousVector;
      }
      // Read present/isNull stream
      stringGroupTreeReader.nextVector(bytesColVector, isNull, batchSize);

      convertVector(bytesColVector, longColVector, batchSize);
    }
  }

  public static class AnyIntegerFromTimestampTreeReader extends ConvertTreeReader {

    private TimestampTreeReader timestampTreeReader;

    private final TypeDescription readerType;
    private TimestampColumnVector timestampColVector;
    private LongColumnVector longColVector;

    AnyIntegerFromTimestampTreeReader(int columnId, TypeDescription readerType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      this.readerType = readerType;
      timestampTreeReader = new TimestampTreeReader(columnId, skipCorrupt);
      setConvertTreeReader(timestampTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      // Use TimestampWritable's getSeconds.
      long longValue = TimestampUtils.millisToSeconds(
          timestampColVector.asScratchTimestamp(elementNum).getTime());
      downCastAnyInteger(longColVector, elementNum, longValue, readerType);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (timestampColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        timestampColVector = new TimestampColumnVector();
        longColVector = (LongColumnVector) previousVector;
      }
      // Read present/isNull stream
      timestampTreeReader.nextVector(timestampColVector, isNull, batchSize);

      convertVector(timestampColVector, longColVector, batchSize);
    }
  }

  public static class FloatFromAnyIntegerTreeReader extends ConvertTreeReader {

    private AnyIntegerTreeReader anyIntegerAsLongTreeReader;

    private LongColumnVector longColVector;
    private DoubleColumnVector doubleColVector;

    FloatFromAnyIntegerTreeReader(int columnId, TypeDescription fileType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      anyIntegerAsLongTreeReader =
          new AnyIntegerTreeReader(columnId, fileType, skipCorrupt);
      setConvertTreeReader(anyIntegerAsLongTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      float floatValue = (float) longColVector.vector[elementNum];
      if (!Float.isNaN(floatValue)) {
        doubleColVector.vector[elementNum] = floatValue;
      } else {
        doubleColVector.vector[elementNum] = Double.NaN;
        doubleColVector.noNulls = false;
        doubleColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new LongColumnVector();
        doubleColVector = (DoubleColumnVector) previousVector;
      }
      // Read present/isNull stream
      anyIntegerAsLongTreeReader.nextVector(longColVector, isNull, batchSize);

      convertVector(longColVector, doubleColVector, batchSize);
    }
  }

  public static class FloatFromDoubleTreeReader extends ConvertTreeReader {

    private DoubleTreeReader doubleTreeReader;

    FloatFromDoubleTreeReader(int columnId) throws IOException {
      super(columnId);
      doubleTreeReader = new DoubleTreeReader(columnId);
      setConvertTreeReader(doubleTreeReader);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      doubleTreeReader.nextVector(previousVector, isNull, batchSize);

      DoubleColumnVector resultColVector = (DoubleColumnVector) previousVector;
      double[] resultVector = resultColVector.vector;
      if (resultColVector.isRepeating) {
        if (resultColVector.noNulls || !resultColVector.isNull[0]) {
          resultVector[0] = (float) resultVector[0];
        } else {
          // Remains null.
        }
      } else if (resultColVector.noNulls){
        for (int i = 0; i < batchSize; i++) {
          resultVector[i] = (float) resultVector[i];
        }
      } else {
        for (int i = 0; i < batchSize; i++) {
          if (!resultColVector.isNull[i]) {
            resultVector[i] = (float) resultVector[i];
          } else {
            // Remains null.
          }
        }
      }
    }
  }

  public static class FloatFromDecimalTreeReader extends ConvertTreeReader {

    private DecimalTreeReader decimalTreeReader;

    private final int precision;
    private final int scale;
    private DecimalColumnVector decimalColVector;
    private DoubleColumnVector doubleColVector;

    FloatFromDecimalTreeReader(int columnId, TypeDescription fileType,
        TypeDescription readerType) throws IOException {
      super(columnId);
      this.precision = fileType.getPrecision();
      this.scale = fileType.getScale();
      decimalTreeReader = new DecimalTreeReader(columnId, precision, scale);
      setConvertTreeReader(decimalTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      doubleColVector.vector[elementNum] =
          (float) decimalColVector.vector[elementNum].getHiveDecimal().doubleValue();
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (decimalColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        decimalColVector = new DecimalColumnVector(precision, scale);
        doubleColVector = (DoubleColumnVector) previousVector;
      }
      // Read present/isNull stream
      decimalTreeReader.nextVector(decimalColVector, isNull, batchSize);

      convertVector(decimalColVector, doubleColVector, batchSize);
    }
  }

  public static class FloatFromStringGroupTreeReader extends ConvertTreeReader {

    private TreeReader stringGroupTreeReader;

    private BytesColumnVector bytesColVector;
    private DoubleColumnVector doubleColVector;

    FloatFromStringGroupTreeReader(int columnId, TypeDescription fileType)
        throws IOException {
      super(columnId);
      stringGroupTreeReader = getStringGroupTreeReader(columnId, fileType);
      setConvertTreeReader(stringGroupTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String string = stringFromBytesColumnVectorEntry(bytesColVector, elementNum);
      float floatValue = parseFloatFromString(string);
      if (!getIsParseError()) {
        doubleColVector.vector[elementNum] = floatValue;
      } else {
        doubleColVector.vector[elementNum] = Double.NaN;
        doubleColVector.noNulls = false;
        doubleColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (bytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        bytesColVector = new BytesColumnVector();
        doubleColVector = (DoubleColumnVector) previousVector;
      }
      // Read present/isNull stream
      stringGroupTreeReader.nextVector(bytesColVector, isNull, batchSize);

      convertVector(bytesColVector, doubleColVector, batchSize);
    }
  }

  public static class FloatFromTimestampTreeReader extends ConvertTreeReader {

    private TimestampTreeReader timestampTreeReader;

    private TimestampColumnVector timestampColVector;
    private DoubleColumnVector doubleColVector;

    FloatFromTimestampTreeReader(int columnId, boolean skipCorrupt) throws IOException {
      super(columnId);
      timestampTreeReader = new TimestampTreeReader(columnId, skipCorrupt);
      setConvertTreeReader(timestampTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      doubleColVector.vector[elementNum] = (float) TimestampUtils.getDouble(
          timestampColVector.asScratchTimestamp(elementNum));
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (timestampColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        timestampColVector = new TimestampColumnVector();
        doubleColVector = (DoubleColumnVector) previousVector;
      }
      // Read present/isNull stream
      timestampTreeReader.nextVector(timestampColVector, isNull, batchSize);

      convertVector(timestampColVector, doubleColVector, batchSize);
    }
  }

  public static class DoubleFromAnyIntegerTreeReader extends ConvertTreeReader {

    private AnyIntegerTreeReader anyIntegerAsLongTreeReader;

    private LongColumnVector longColVector;
    private DoubleColumnVector doubleColVector;

    DoubleFromAnyIntegerTreeReader(int columnId, TypeDescription fileType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      anyIntegerAsLongTreeReader =
          new AnyIntegerTreeReader(columnId, fileType, skipCorrupt);
      setConvertTreeReader(anyIntegerAsLongTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {

      double doubleValue = (double) longColVector.vector[elementNum];
      if (!Double.isNaN(doubleValue)) {
        doubleColVector.vector[elementNum] = doubleValue;
      } else {
        doubleColVector.vector[elementNum] = Double.NaN;
        doubleColVector.noNulls = false;
        doubleColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new LongColumnVector();
        doubleColVector = (DoubleColumnVector) previousVector;
      }
      // Read present/isNull stream
      anyIntegerAsLongTreeReader.nextVector(longColVector, isNull, batchSize);

      convertVector(longColVector, doubleColVector, batchSize);
    }
  }

  public static class DoubleFromFloatTreeReader extends ConvertTreeReader {

    private FloatTreeReader floatTreeReader;

    DoubleFromFloatTreeReader(int columnId) throws IOException {
      super(columnId);
      floatTreeReader = new FloatTreeReader(columnId);
      setConvertTreeReader(floatTreeReader);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      // we get the DoubleColumnVector produced by float tree reader first, then iterate through
      // the elements and make double -> float -> string -> double conversion to preserve the
      // precision. When float tree reader reads float and assign it to double, java's widening
      // conversion adds more precision which will break all comparisons.
      // Example: float f = 74.72
      // double d = f ---> 74.72000122070312
      // Double.parseDouble(String.valueOf(f)) ---> 74.72
      floatTreeReader.nextVector(previousVector, isNull, batchSize);

      DoubleColumnVector doubleColumnVector = (DoubleColumnVector) previousVector;
      if (doubleColumnVector.isRepeating) {
        if (doubleColumnVector.noNulls || !doubleColumnVector.isNull[0]) {
          final float f = (float) doubleColumnVector.vector[0];
          doubleColumnVector.vector[0] = Double.parseDouble(String.valueOf(f));
        }
      } else if (doubleColumnVector.noNulls){
        for (int i = 0; i < batchSize; i++) {
          final float f = (float) doubleColumnVector.vector[i];
          doubleColumnVector.vector[i] = Double.parseDouble(String.valueOf(f));
        }
      } else {
        for (int i = 0; i < batchSize; i++) {
          if (!doubleColumnVector.isNull[i]) {
            final float f = (float) doubleColumnVector.vector[i];
            doubleColumnVector.vector[i] = Double.parseDouble(String.valueOf(f));
          }
        }
      }
    }
  }

  public static class DoubleFromDecimalTreeReader extends ConvertTreeReader {

    private DecimalTreeReader decimalTreeReader;

    private final int precision;
    private final int scale;
    private DecimalColumnVector decimalColVector;
    private DoubleColumnVector doubleColVector;

    DoubleFromDecimalTreeReader(int columnId, TypeDescription fileType) throws IOException {
      super(columnId);
      this.precision = fileType.getPrecision();
      this.scale = fileType.getScale();
      decimalTreeReader = new DecimalTreeReader(columnId, precision, scale);
      setConvertTreeReader(decimalTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      doubleColVector.vector[elementNum] =
          decimalColVector.vector[elementNum].getHiveDecimal().doubleValue();
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (decimalColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        decimalColVector = new DecimalColumnVector(precision, scale);
        doubleColVector = (DoubleColumnVector) previousVector;
      }
      // Read present/isNull stream
      decimalTreeReader.nextVector(decimalColVector, isNull, batchSize);

      convertVector(decimalColVector, doubleColVector, batchSize);
    }
  }

  public static class DoubleFromStringGroupTreeReader extends ConvertTreeReader {

    private TreeReader stringGroupTreeReader;

    private BytesColumnVector bytesColVector;
    private DoubleColumnVector doubleColVector;

    DoubleFromStringGroupTreeReader(int columnId, TypeDescription fileType)
        throws IOException {
      super(columnId);
      stringGroupTreeReader = getStringGroupTreeReader(columnId, fileType);
      setConvertTreeReader(stringGroupTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String string = stringFromBytesColumnVectorEntry(bytesColVector, elementNum);
      double doubleValue = parseDoubleFromString(string);
      if (!getIsParseError()) {
        doubleColVector.vector[elementNum] = doubleValue;
      } else {
        doubleColVector.noNulls = false;
        doubleColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (bytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        bytesColVector = new BytesColumnVector();
        doubleColVector = (DoubleColumnVector) previousVector;
      }
      // Read present/isNull stream
      stringGroupTreeReader.nextVector(bytesColVector, isNull, batchSize);

      convertVector(bytesColVector, doubleColVector, batchSize);
    }
  }

  public static class DoubleFromTimestampTreeReader extends ConvertTreeReader {

    private TimestampTreeReader timestampTreeReader;

    private TimestampColumnVector timestampColVector;
    private DoubleColumnVector doubleColVector;

    DoubleFromTimestampTreeReader(int columnId, boolean skipCorrupt) throws IOException {
      super(columnId);
      timestampTreeReader = new TimestampTreeReader(columnId, skipCorrupt);
      setConvertTreeReader(timestampTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      doubleColVector.vector[elementNum] = TimestampUtils.getDouble(
          timestampColVector.asScratchTimestamp(elementNum));
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (timestampColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        timestampColVector = new TimestampColumnVector();
        doubleColVector = (DoubleColumnVector) previousVector;
      }
      // Read present/isNull stream
      timestampTreeReader.nextVector(timestampColVector, isNull, batchSize);

      convertVector(timestampColVector, doubleColVector, batchSize);
    }
  }

  public static class DecimalFromAnyIntegerTreeReader extends ConvertTreeReader {

    private AnyIntegerTreeReader anyIntegerAsLongTreeReader;

    private LongColumnVector longColVector;
    private DecimalColumnVector decimalColVector;

    DecimalFromAnyIntegerTreeReader(int columnId, TypeDescription fileType, boolean skipCorrupt)
        throws IOException {
      super(columnId);
      anyIntegerAsLongTreeReader =
          new AnyIntegerTreeReader(columnId, fileType, skipCorrupt);
      setConvertTreeReader(anyIntegerAsLongTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      long longValue = longColVector.vector[elementNum];
      HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(longValue);
      // The DecimalColumnVector will enforce precision and scale and set the entry to null when out of bounds.
      decimalColVector.set(elementNum, hiveDecimalWritable);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
        boolean[] isNull,
        final int batchSize) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new LongColumnVector();
        decimalColVector = (DecimalColumnVector) previousVector;
      }
      // Read present/isNull stream
      anyIntegerAsLongTreeReader.nextVector(longColVector, isNull, batchSize);

      convertVector(longColVector, decimalColVector, batchSize);
    }
  }

  public static class DecimalFromFloatTreeReader extends ConvertTreeReader {

    private FloatTreeReader floatTreeReader;

    private DoubleColumnVector doubleColVector;
    private DecimalColumnVector decimalColVector;

    DecimalFromFloatTreeReader(int columnId, TypeDescription readerType)
        throws IOException {
      super(columnId);
      floatTreeReader = new FloatTreeReader(columnId);
      setConvertTreeReader(floatTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      float floatValue = (float) doubleColVector.vector[elementNum];
      if (!Float.isNaN(floatValue)) {
        HiveDecimal decimalValue =
            HiveDecimal.create(Float.toString(floatValue));
        if (decimalValue != null) {
          // The DecimalColumnVector will enforce precision and scale and set the entry to null when out of bounds.
          decimalColVector.set(elementNum, decimalValue);
        } else {
          decimalColVector.noNulls = false;
          decimalColVector.isNull[elementNum] = true;
        }
      } else {
        decimalColVector.noNulls = false;
        decimalColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector();
        decimalColVector = (DecimalColumnVector) previousVector;
      }
      // Read present/isNull stream
      floatTreeReader.nextVector(doubleColVector, isNull, batchSize);

      convertVector(doubleColVector, decimalColVector, batchSize);
    }
  }

  public static class DecimalFromDoubleTreeReader extends ConvertTreeReader {

    private DoubleTreeReader doubleTreeReader;

    private DoubleColumnVector doubleColVector;
    private DecimalColumnVector decimalColVector;

    DecimalFromDoubleTreeReader(int columnId, TypeDescription readerType)
        throws IOException {
      super(columnId);
      doubleTreeReader = new DoubleTreeReader(columnId);
      setConvertTreeReader(doubleTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      HiveDecimal value =
          HiveDecimal.create(Double.toString(doubleColVector.vector[elementNum]));
      if (value != null) {
        decimalColVector.set(elementNum, value);
      } else {
        decimalColVector.noNulls = false;
        decimalColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector();
        decimalColVector = (DecimalColumnVector) previousVector;
      }
      // Read present/isNull stream
      doubleTreeReader.nextVector(doubleColVector, isNull, batchSize);

      convertVector(doubleColVector, decimalColVector, batchSize);
    }
  }

  public static class DecimalFromStringGroupTreeReader extends ConvertTreeReader {

    private TreeReader stringGroupTreeReader;

    private BytesColumnVector bytesColVector;
    private DecimalColumnVector decimalColVector;

    DecimalFromStringGroupTreeReader(int columnId, TypeDescription fileType,
        TypeDescription readerType) throws IOException {
      super(columnId);
      stringGroupTreeReader = getStringGroupTreeReader(columnId, fileType);
      setConvertTreeReader(stringGroupTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String string = stringFromBytesColumnVectorEntry(bytesColVector, elementNum);
      HiveDecimal value = parseDecimalFromString(string);
      if (value != null) {
        // The DecimalColumnVector will enforce precision and scale and set the entry to null when out of bounds.
        decimalColVector.set(elementNum, value);
      } else {
        decimalColVector.noNulls = false;
        decimalColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (bytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        bytesColVector = new BytesColumnVector();
        decimalColVector = (DecimalColumnVector) previousVector;
      }
      // Read present/isNull stream
      stringGroupTreeReader.nextVector(bytesColVector, isNull, batchSize);

      convertVector(bytesColVector, decimalColVector, batchSize);
    }
  }

  public static class DecimalFromTimestampTreeReader extends ConvertTreeReader {

    private TimestampTreeReader timestampTreeReader;

    private TimestampColumnVector timestampColVector;
    private DecimalColumnVector decimalColVector;

    DecimalFromTimestampTreeReader(int columnId, boolean skipCorrupt) throws IOException {
      super(columnId);
      timestampTreeReader = new TimestampTreeReader(columnId, skipCorrupt);
      setConvertTreeReader(timestampTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      double doubleValue = TimestampUtils.getDouble(
          timestampColVector.asScratchTimestamp(elementNum));
      HiveDecimal value = HiveDecimal.create(Double.toString(doubleValue));
      if (value != null) {
        // The DecimalColumnVector will enforce precision and scale and set the entry to null when out of bounds.
        decimalColVector.set(elementNum, value);
      } else {
        decimalColVector.noNulls = false;
        decimalColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (timestampColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        timestampColVector = new TimestampColumnVector();
        decimalColVector = (DecimalColumnVector) previousVector;
      }
      // Read present/isNull stream
      timestampTreeReader.nextVector(timestampColVector, isNull, batchSize);

      convertVector(timestampColVector, decimalColVector, batchSize);
    }
  }

  public static class DecimalFromDecimalTreeReader extends ConvertTreeReader {

    private DecimalTreeReader decimalTreeReader;

    private DecimalColumnVector fileDecimalColVector;
    private int filePrecision;
    private int fileScale;
    private int readerPrecision;
    private int readerScale;
    private DecimalColumnVector decimalColVector;

    DecimalFromDecimalTreeReader(int columnId, TypeDescription fileType, TypeDescription readerType)
        throws IOException {
      super(columnId);
      filePrecision = fileType.getPrecision();
      fileScale = fileType.getScale();
      readerPrecision = readerType.getPrecision();
      readerScale = readerType.getScale();
      decimalTreeReader = new DecimalTreeReader(columnId, filePrecision, fileScale);
      setConvertTreeReader(decimalTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {

      HiveDecimalWritable valueWritable = HiveDecimalWritable.enforcePrecisionScale(
          fileDecimalColVector.vector[elementNum], readerPrecision, readerScale);
      if (valueWritable != null) {
        decimalColVector.set(elementNum, valueWritable);
      } else {
        decimalColVector.noNulls = false;
        decimalColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (fileDecimalColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        fileDecimalColVector = new DecimalColumnVector(filePrecision, fileScale);
        decimalColVector = (DecimalColumnVector) previousVector;
      }
      // Read present/isNull stream
      decimalTreeReader.nextVector(fileDecimalColVector, isNull, batchSize);

      convertVector(fileDecimalColVector, decimalColVector, batchSize);
    }
  }

  public static class StringGroupFromAnyIntegerTreeReader extends ConvertTreeReader {

    private AnyIntegerTreeReader anyIntegerAsLongTreeReader;

    private final TypeDescription readerType;
    private LongColumnVector longColVector;
    private BytesColumnVector bytesColVector;

    StringGroupFromAnyIntegerTreeReader(int columnId, TypeDescription fileType,
        TypeDescription readerType, boolean skipCorrupt) throws IOException {
      super(columnId);
      this.readerType = readerType;
      anyIntegerAsLongTreeReader =
          new AnyIntegerTreeReader(columnId, fileType, skipCorrupt);
      setConvertTreeReader(anyIntegerAsLongTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      long longValue = longColVector.vector[elementNum];
      String string = anyIntegerAsLongTreeReader.getString(longValue);
      byte[] bytes = string.getBytes();
      assignStringGroupVectorEntry(bytesColVector, elementNum, readerType, bytes);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new LongColumnVector();
        bytesColVector = (BytesColumnVector) previousVector;
      }
      // Read present/isNull stream
      anyIntegerAsLongTreeReader.nextVector(longColVector, isNull, batchSize);

      convertVector(longColVector, bytesColVector, batchSize);
    }
  }

  public static class StringGroupFromFloatTreeReader extends ConvertTreeReader {

    private FloatTreeReader floatTreeReader;

    private final TypeDescription readerType;
    private DoubleColumnVector doubleColVector;
    private BytesColumnVector bytesColVector;


    StringGroupFromFloatTreeReader(int columnId, TypeDescription readerType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      this.readerType = readerType;
      floatTreeReader = new FloatTreeReader(columnId);
      setConvertTreeReader(floatTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      float floatValue = (float) doubleColVector.vector[elementNum];
      if (!Float.isNaN(floatValue)) {
        String string = String.valueOf(floatValue);
        byte[] bytes = string.getBytes();
        assignStringGroupVectorEntry(bytesColVector, elementNum, readerType, bytes);
      } else {
        bytesColVector.noNulls = false;
        bytesColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector();
        bytesColVector = (BytesColumnVector) previousVector;
      }
      // Read present/isNull stream
      floatTreeReader.nextVector(doubleColVector, isNull, batchSize);

      convertVector(doubleColVector, bytesColVector, batchSize);
    }
  }

  public static class StringGroupFromDoubleTreeReader extends ConvertTreeReader {

    private DoubleTreeReader doubleTreeReader;

    private final TypeDescription readerType;
    private DoubleColumnVector doubleColVector;
    private BytesColumnVector bytesColVector;

    StringGroupFromDoubleTreeReader(int columnId, TypeDescription readerType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      this.readerType = readerType;
      doubleTreeReader = new DoubleTreeReader(columnId);
      setConvertTreeReader(doubleTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      double doubleValue = doubleColVector.vector[elementNum];
      if (!Double.isNaN(doubleValue)) {
        String string = String.valueOf(doubleValue);
        byte[] bytes = string.getBytes();
        assignStringGroupVectorEntry(bytesColVector, elementNum, readerType, bytes);
      } else {
        bytesColVector.noNulls = false;
        bytesColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector();
        bytesColVector = (BytesColumnVector) previousVector;
      }
      // Read present/isNull stream
      doubleTreeReader.nextVector(doubleColVector, isNull, batchSize);

      convertVector(doubleColVector, bytesColVector, batchSize);
    }
  }



  public static class StringGroupFromDecimalTreeReader extends ConvertTreeReader {

    private DecimalTreeReader decimalTreeReader;

    private int precision;
    private int scale;
    private final TypeDescription readerType;
    private DecimalColumnVector decimalColVector;
    private BytesColumnVector bytesColVector;

    StringGroupFromDecimalTreeReader(int columnId, TypeDescription fileType,
        TypeDescription readerType, boolean skipCorrupt) throws IOException {
      super(columnId);
      this.precision = fileType.getPrecision();
      this.scale = fileType.getScale();
      this.readerType = readerType;
      decimalTreeReader = new DecimalTreeReader(columnId, precision, scale);
      setConvertTreeReader(decimalTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      String string = decimalColVector.vector[elementNum].getHiveDecimal().toString();
      byte[] bytes = string.getBytes();
      assignStringGroupVectorEntry(bytesColVector, elementNum, readerType, bytes);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (decimalColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        decimalColVector = new DecimalColumnVector(precision, scale);
        bytesColVector = (BytesColumnVector) previousVector;
      }
      // Read present/isNull stream
      decimalTreeReader.nextVector(decimalColVector, isNull, batchSize);

      convertVector(decimalColVector, bytesColVector, batchSize);
    }
  }

  public static class StringGroupFromTimestampTreeReader extends ConvertTreeReader {

    private TimestampTreeReader timestampTreeReader;

    private final TypeDescription readerType;
    private TimestampColumnVector timestampColVector;
    private BytesColumnVector bytesColVector;

    StringGroupFromTimestampTreeReader(int columnId, TypeDescription readerType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      this.readerType = readerType;
      timestampTreeReader = new TimestampTreeReader(columnId, skipCorrupt);
      setConvertTreeReader(timestampTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String string =
          timestampColVector.asScratchTimestamp(elementNum).toString();
      byte[] bytes = string.getBytes();
      assignStringGroupVectorEntry(bytesColVector, elementNum, readerType, bytes);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (timestampColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        timestampColVector = new TimestampColumnVector();
        bytesColVector = (BytesColumnVector) previousVector;
      }
      // Read present/isNull stream
      timestampTreeReader.nextVector(timestampColVector, isNull, batchSize);

      convertVector(timestampColVector, bytesColVector, batchSize);
    }
  }

  public static class StringGroupFromDateTreeReader extends ConvertTreeReader {

    private DateTreeReader dateTreeReader;

    private final TypeDescription readerType;
    private LongColumnVector longColVector;
    private BytesColumnVector bytesColVector;
    private Date date;

    StringGroupFromDateTreeReader(int columnId, TypeDescription readerType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      this.readerType = readerType;
      dateTreeReader = new DateTreeReader(columnId);
      setConvertTreeReader(dateTreeReader);
      date = new Date(0);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      date.setTime(DateWritable.daysToMillis((int) longColVector.vector[elementNum]));
      String string = date.toString();
      byte[] bytes = string.getBytes();
      assignStringGroupVectorEntry(bytesColVector, elementNum, readerType, bytes);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new LongColumnVector();
        bytesColVector = (BytesColumnVector) previousVector;
      }
      // Read present/isNull stream
      dateTreeReader.nextVector(longColVector, isNull, batchSize);

      convertVector(longColVector, bytesColVector, batchSize);
    }
  }

  public static class StringGroupFromStringGroupTreeReader extends ConvertTreeReader {

    private TreeReader stringGroupTreeReader;

    private final TypeDescription readerType;

    StringGroupFromStringGroupTreeReader(int columnId, TypeDescription fileType,
        TypeDescription readerType) throws IOException {
      super(columnId);
      this.readerType = readerType;
      stringGroupTreeReader = getStringGroupTreeReader(columnId, fileType);
      setConvertTreeReader(stringGroupTreeReader);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      stringGroupTreeReader.nextVector(previousVector, isNull, batchSize);

      BytesColumnVector resultColVector = (BytesColumnVector) previousVector;

      if (resultColVector.isRepeating) {
        if (resultColVector.noNulls || !resultColVector.isNull[0]) {
          convertStringGroupVectorElement(resultColVector, 0, readerType);
        } else {
          // Remains null.
        }
      } else if (resultColVector.noNulls){
        for (int i = 0; i < batchSize; i++) {
          convertStringGroupVectorElement(resultColVector, i, readerType);
        }
      } else {
        for (int i = 0; i < batchSize; i++) {
          if (!resultColVector.isNull[i]) {
            convertStringGroupVectorElement(resultColVector, i, readerType);
          } else {
            // Remains null.
          }
        }
      }
    }
  }

  public static class StringGroupFromBinaryTreeReader extends ConvertTreeReader {

    private BinaryTreeReader binaryTreeReader;

    private final TypeDescription readerType;
    private BytesColumnVector inBytesColVector;
    private BytesColumnVector outBytesColVector;

    StringGroupFromBinaryTreeReader(int columnId, TypeDescription readerType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      this.readerType = readerType;
      binaryTreeReader = new BinaryTreeReader(columnId);
      setConvertTreeReader(binaryTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      byte[] bytes = inBytesColVector.vector[elementNum];
      int start = inBytesColVector.start[elementNum];
      int length = inBytesColVector.length[elementNum];
      byte[] string = new byte[length == 0 ? 0 : 3 * length - 1];
      for(int p = 0; p < string.length; p += 2) {
        if (p != 0) {
          string[p++] = ' ';
        }
        int num = 0xff & bytes[start++];
        int digit = num / 16;
        string[p] = (byte)((digit) + (digit < 10 ? '0' : 'a' - 10));
        digit = num % 16;
        string[p + 1] = (byte)((digit) + (digit < 10 ? '0' : 'a' - 10));
      }
      assignStringGroupVectorEntry(outBytesColVector, elementNum, readerType,
          string, 0, string.length);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (inBytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        inBytesColVector = new BytesColumnVector();
        outBytesColVector = (BytesColumnVector) previousVector;
      }
      // Read present/isNull stream
      binaryTreeReader.nextVector(inBytesColVector, isNull, batchSize);

      convertVector(inBytesColVector, outBytesColVector, batchSize);
    }
  }

  public static class TimestampFromAnyIntegerTreeReader extends ConvertTreeReader {

    private AnyIntegerTreeReader anyIntegerAsLongTreeReader;

    private LongColumnVector longColVector;
    private TimestampColumnVector timestampColVector;

    TimestampFromAnyIntegerTreeReader(int columnId, TypeDescription fileType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      anyIntegerAsLongTreeReader =
          new AnyIntegerTreeReader(columnId, fileType, skipCorrupt);
      setConvertTreeReader(anyIntegerAsLongTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      long longValue = longColVector.vector[elementNum];
      // UNDONE: What does the boolean setting need to be?
      timestampColVector.set(elementNum, new Timestamp(longValue));
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new LongColumnVector();
        timestampColVector = (TimestampColumnVector) previousVector;
      }
      // Read present/isNull stream
      anyIntegerAsLongTreeReader.nextVector(longColVector, isNull, batchSize);

      convertVector(longColVector, timestampColVector, batchSize);
    }
  }

  public static class TimestampFromFloatTreeReader extends ConvertTreeReader {

    private FloatTreeReader floatTreeReader;

    private DoubleColumnVector doubleColVector;
    private TimestampColumnVector timestampColVector;

    TimestampFromFloatTreeReader(int columnId, TypeDescription fileType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      floatTreeReader = new FloatTreeReader(columnId);
      setConvertTreeReader(floatTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      float floatValue = (float) doubleColVector.vector[elementNum];
      Timestamp timestampValue = TimestampUtils.doubleToTimestamp(floatValue);
      // The TimestampColumnVector will set the entry to null when a null timestamp is passed in.
      timestampColVector.set(elementNum, timestampValue);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector();
        timestampColVector = (TimestampColumnVector) previousVector;
      }
      // Read present/isNull stream
      floatTreeReader.nextVector(doubleColVector, isNull, batchSize);

      convertVector(doubleColVector, timestampColVector, batchSize);
    }
  }

  public static class TimestampFromDoubleTreeReader extends ConvertTreeReader {

    private DoubleTreeReader doubleTreeReader;

    private DoubleColumnVector doubleColVector;
    private TimestampColumnVector timestampColVector;

    TimestampFromDoubleTreeReader(int columnId, TypeDescription fileType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      doubleTreeReader = new DoubleTreeReader(columnId);
      setConvertTreeReader(doubleTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      double doubleValue = doubleColVector.vector[elementNum];
      Timestamp timestampValue = TimestampUtils.doubleToTimestamp(doubleValue);
      // The TimestampColumnVector will set the entry to null when a null timestamp is passed in.
      timestampColVector.set(elementNum, timestampValue);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector();
        timestampColVector = (TimestampColumnVector) previousVector;
      }
      // Read present/isNull stream
      doubleTreeReader.nextVector(doubleColVector, isNull, batchSize);

      convertVector(doubleColVector, timestampColVector, batchSize);
    }
  }

  public static class TimestampFromDecimalTreeReader extends ConvertTreeReader {

    private DecimalTreeReader decimalTreeReader;

    private final int precision;
    private final int scale;
    private DecimalColumnVector decimalColVector;
    private TimestampColumnVector timestampColVector;

    TimestampFromDecimalTreeReader(int columnId, TypeDescription fileType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      this.precision = fileType.getPrecision();
      this.scale = fileType.getScale();
      decimalTreeReader = new DecimalTreeReader(columnId, precision, scale);
      setConvertTreeReader(decimalTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      Timestamp timestampValue =
            TimestampUtils.decimalToTimestamp(
                decimalColVector.vector[elementNum].getHiveDecimal());
      // The TimestampColumnVector will set the entry to null when a null timestamp is passed in.
      timestampColVector.set(elementNum, timestampValue);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (decimalColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        decimalColVector = new DecimalColumnVector(precision, scale);
        timestampColVector = (TimestampColumnVector) previousVector;
      }
      // Read present/isNull stream
      decimalTreeReader.nextVector(decimalColVector, isNull, batchSize);

      convertVector(decimalColVector, timestampColVector, batchSize);
    }
  }

  public static class TimestampFromStringGroupTreeReader extends ConvertTreeReader {

    private TreeReader stringGroupTreeReader;

    private BytesColumnVector bytesColVector;
    private TimestampColumnVector timestampColVector;

    TimestampFromStringGroupTreeReader(int columnId, TypeDescription fileType)
        throws IOException {
      super(columnId);
      stringGroupTreeReader = getStringGroupTreeReader(columnId, fileType);
      setConvertTreeReader(stringGroupTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String stringValue =
          stringFromBytesColumnVectorEntry(bytesColVector, elementNum);
      Timestamp timestampValue = parseTimestampFromString(stringValue);
      if (timestampValue != null) {
        timestampColVector.set(elementNum, timestampValue);
      } else {
        timestampColVector.noNulls = false;
        timestampColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (bytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        bytesColVector = new BytesColumnVector();
        timestampColVector = (TimestampColumnVector) previousVector;
      }
      // Read present/isNull stream
      stringGroupTreeReader.nextVector(bytesColVector, isNull, batchSize);

      convertVector(bytesColVector, timestampColVector, batchSize);
    }
  }

  public static class TimestampFromDateTreeReader extends ConvertTreeReader {

    private DateTreeReader dateTreeReader;

    private LongColumnVector longColVector;
    private TimestampColumnVector timestampColVector;

    TimestampFromDateTreeReader(int columnId, TypeDescription fileType,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      dateTreeReader = new DateTreeReader(columnId);
      setConvertTreeReader(dateTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      long millis =
          DateWritable.daysToMillis((int) longColVector.vector[elementNum]);
      timestampColVector.set(elementNum, new Timestamp(millis));
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new LongColumnVector();
        timestampColVector = (TimestampColumnVector) previousVector;
      }
      // Read present/isNull stream
      dateTreeReader.nextVector(longColVector, isNull, batchSize);

      convertVector(longColVector, timestampColVector, batchSize);
    }
  }

  public static class DateFromStringGroupTreeReader extends ConvertTreeReader {

    private TreeReader stringGroupTreeReader;

    private BytesColumnVector bytesColVector;
    private LongColumnVector longColVector;

    DateFromStringGroupTreeReader(int columnId, TypeDescription fileType)
        throws IOException {
      super(columnId);
      stringGroupTreeReader = getStringGroupTreeReader(columnId, fileType);
      setConvertTreeReader(stringGroupTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String stringValue =
          stringFromBytesColumnVectorEntry(bytesColVector, elementNum);
      Date dateValue = parseDateFromString(stringValue);
      if (dateValue != null) {
        longColVector.vector[elementNum] = DateWritable.dateToDays(dateValue);
      } else {
        longColVector.noNulls = false;
        longColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (bytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        bytesColVector = new BytesColumnVector();
        longColVector = (LongColumnVector) previousVector;
      }
      // Read present/isNull stream
      stringGroupTreeReader.nextVector(bytesColVector, isNull, batchSize);

      convertVector(bytesColVector, longColVector, batchSize);
    }
  }

  public static class DateFromTimestampTreeReader extends ConvertTreeReader {

    private TimestampTreeReader timestampTreeReader;

    private TimestampColumnVector timestampColVector;
    private LongColumnVector longColVector;

    DateFromTimestampTreeReader(int columnId, boolean skipCorrupt) throws IOException {
      super(columnId);
      timestampTreeReader = new TimestampTreeReader(columnId, skipCorrupt);
      setConvertTreeReader(timestampTreeReader);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      Date dateValue =
          DateWritable.timeToDate(TimestampUtils.millisToSeconds(
              timestampColVector.asScratchTimestamp(elementNum).getTime()));
      longColVector.vector[elementNum] = DateWritable.dateToDays(dateValue);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (timestampColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        timestampColVector = new TimestampColumnVector();
        longColVector = (LongColumnVector) previousVector;
      }
      // Read present/isNull stream
      timestampTreeReader.nextVector(timestampColVector, isNull, batchSize);

      convertVector(timestampColVector, longColVector, batchSize);
    }
  }

  public static class BinaryFromStringGroupTreeReader extends ConvertTreeReader {

    private TreeReader stringGroupTreeReader;

    BinaryFromStringGroupTreeReader(int columnId, TypeDescription fileType)
        throws IOException {
      super(columnId);
      stringGroupTreeReader = getStringGroupTreeReader(columnId, fileType);
      setConvertTreeReader(stringGroupTreeReader);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      super.nextVector(previousVector, isNull, batchSize);
    }
  }

  private static TreeReader createAnyIntegerConvertTreeReader(int columnId,
                                                              TypeDescription fileType,
                                                              TypeDescription readerType,
                                                              SchemaEvolution evolution,
                                                              boolean[] included,
                                                              boolean skipCorrupt) throws IOException {

    // CONVERT from (BOOLEAN, BYTE, SHORT, INT, LONG) to schema type.
    //
    switch (readerType.getCategory()) {

    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      if (fileType.getCategory() == readerType.getCategory()) {
        throw new IllegalArgumentException("No conversion of type " +
            readerType.getCategory() + " to self needed");
      }
      return new AnyIntegerFromAnyIntegerTreeReader(columnId, fileType, readerType,
          skipCorrupt);

    case FLOAT:
      return new FloatFromAnyIntegerTreeReader(columnId, fileType,
          skipCorrupt);

    case DOUBLE:
      return new DoubleFromAnyIntegerTreeReader(columnId, fileType,
          skipCorrupt);

    case DECIMAL:
      return new DecimalFromAnyIntegerTreeReader(columnId, fileType, skipCorrupt);

    case STRING:
    case CHAR:
    case VARCHAR:
      return new StringGroupFromAnyIntegerTreeReader(columnId, fileType, readerType,
          skipCorrupt);

    case TIMESTAMP:
      return new TimestampFromAnyIntegerTreeReader(columnId, fileType, skipCorrupt);

    // Not currently supported conversion(s):
    case BINARY:
    case DATE:

    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new IllegalArgumentException("Unsupported type " +
          readerType.getCategory());
    }
  }

  private static TreeReader createFloatConvertTreeReader(int columnId,
                                                         TypeDescription fileType,
                                                         TypeDescription readerType,
                                                         SchemaEvolution evolution,
                                                         boolean[] included,
                                                         boolean skipCorrupt) throws IOException {

    // CONVERT from FLOAT to schema type.
    switch (readerType.getCategory()) {

    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return new AnyIntegerFromFloatTreeReader(columnId, readerType);

    case FLOAT:
      throw new IllegalArgumentException("No conversion of type " +
        readerType.getCategory() + " to self needed");

    case DOUBLE:
      return new DoubleFromFloatTreeReader(columnId);

    case DECIMAL:
      return new DecimalFromFloatTreeReader(columnId, readerType);

    case STRING:
    case CHAR:
    case VARCHAR:
      return new StringGroupFromFloatTreeReader(columnId, readerType, skipCorrupt);

    case TIMESTAMP:
      return new TimestampFromFloatTreeReader(columnId, readerType, skipCorrupt);

    // Not currently supported conversion(s):
    case BINARY:
    case DATE:

    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new IllegalArgumentException("Unsupported type " +
          readerType.getCategory());
    }
  }

  private static TreeReader createDoubleConvertTreeReader(int columnId,
                                                          TypeDescription fileType,
                                                          TypeDescription readerType,
                                                          SchemaEvolution evolution,
                                                          boolean[] included,
                                                          boolean skipCorrupt) throws IOException {

    // CONVERT from DOUBLE to schema type.
    switch (readerType.getCategory()) {

    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return new AnyIntegerFromDoubleTreeReader(columnId, readerType);

    case FLOAT:
      return new FloatFromDoubleTreeReader(columnId);

    case DOUBLE:
      throw new IllegalArgumentException("No conversion of type " +
        readerType.getCategory() + " to self needed");

    case DECIMAL:
      return new DecimalFromDoubleTreeReader(columnId, readerType);

    case STRING:
    case CHAR:
    case VARCHAR:
      return new StringGroupFromDoubleTreeReader(columnId, readerType, skipCorrupt);

    case TIMESTAMP:
      return new TimestampFromDoubleTreeReader(columnId, readerType, skipCorrupt);

    // Not currently supported conversion(s):
    case BINARY:
    case DATE:

    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new IllegalArgumentException("Unsupported type " +
          readerType.getCategory());
    }
  }

  private static TreeReader createDecimalConvertTreeReader(int columnId,
                                                           TypeDescription fileType,
                                                           TypeDescription readerType,
                                                           SchemaEvolution evolution,
                                                           boolean[] included,
                                                           boolean skipCorrupt) throws IOException {

    // CONVERT from DECIMAL to schema type.
    switch (readerType.getCategory()) {

    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return new AnyIntegerFromDecimalTreeReader(columnId, fileType, readerType);

    case FLOAT:
      return new FloatFromDecimalTreeReader(columnId, fileType, readerType);

    case DOUBLE:
      return new DoubleFromDecimalTreeReader(columnId, fileType);

    case STRING:
    case CHAR:
    case VARCHAR:
      return new StringGroupFromDecimalTreeReader(columnId, fileType, readerType, skipCorrupt);

    case TIMESTAMP:
      return new TimestampFromDecimalTreeReader(columnId, fileType, skipCorrupt);

    case DECIMAL:
      return new DecimalFromDecimalTreeReader(columnId, fileType, readerType);

    // Not currently supported conversion(s):
    case BINARY:
    case DATE:

    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new IllegalArgumentException("Unsupported type " +
          readerType.getCategory());
    }
  }

  private static TreeReader createStringConvertTreeReader(int columnId,
                                                          TypeDescription fileType,
                                                          TypeDescription readerType,
                                                          SchemaEvolution evolution,
                                                          boolean[] included,
                                                          boolean skipCorrupt) throws IOException {

    // CONVERT from STRING to schema type.
    switch (readerType.getCategory()) {

    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return new AnyIntegerFromStringGroupTreeReader(columnId, fileType, readerType);

    case FLOAT:
      return new FloatFromStringGroupTreeReader(columnId, fileType);

    case DOUBLE:
      return new DoubleFromStringGroupTreeReader(columnId, fileType);

    case DECIMAL:
      return new DecimalFromStringGroupTreeReader(columnId, fileType, readerType);

    case CHAR:
      return new StringGroupFromStringGroupTreeReader(columnId, fileType, readerType);

    case VARCHAR:
      return new StringGroupFromStringGroupTreeReader(columnId, fileType, readerType);

    case STRING:
      throw new IllegalArgumentException("No conversion of type " +
          readerType.getCategory() + " to self needed");

    case BINARY:
      return new BinaryFromStringGroupTreeReader(columnId, fileType);

    case TIMESTAMP:
      return new TimestampFromStringGroupTreeReader(columnId, fileType);

    case DATE:
      return new DateFromStringGroupTreeReader(columnId, fileType);

    // Not currently supported conversion(s):

    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new IllegalArgumentException("Unsupported type " +
          readerType.getCategory());
    }
  }

  private static TreeReader createCharConvertTreeReader(int columnId,
                                                        TypeDescription fileType,
                                                        TypeDescription readerType,
                                                        SchemaEvolution evolution,
                                                        boolean[] included,
                                                        boolean skipCorrupt) throws IOException {

    // CONVERT from CHAR to schema type.
    switch (readerType.getCategory()) {

    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return new AnyIntegerFromStringGroupTreeReader(columnId, fileType, readerType);

    case FLOAT:
      return new FloatFromStringGroupTreeReader(columnId, fileType);

    case DOUBLE:
      return new DoubleFromStringGroupTreeReader(columnId, fileType);

    case DECIMAL:
      return new DecimalFromStringGroupTreeReader(columnId, fileType, readerType);

    case STRING:
      return new StringGroupFromStringGroupTreeReader(columnId, fileType, readerType);

    case VARCHAR:
      return new StringGroupFromStringGroupTreeReader(columnId, fileType, readerType);

    case CHAR:
      return new StringGroupFromStringGroupTreeReader(columnId, fileType, readerType);

    case BINARY:
      return new BinaryFromStringGroupTreeReader(columnId, fileType);

    case TIMESTAMP:
      return new TimestampFromStringGroupTreeReader(columnId, fileType);

    case DATE:
      return new DateFromStringGroupTreeReader(columnId, fileType);

    // Not currently supported conversion(s):

    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new IllegalArgumentException("Unsupported type " +
          readerType.getCategory());
    }
  }

  private static TreeReader createVarcharConvertTreeReader(int columnId,
                                                           TypeDescription fileType,
                                                           TypeDescription readerType,
                                                           SchemaEvolution evolution,
                                                           boolean[] included,
                                                           boolean skipCorrupt) throws IOException {

    // CONVERT from VARCHAR to schema type.
    switch (readerType.getCategory()) {

    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return new AnyIntegerFromStringGroupTreeReader(columnId, fileType, readerType);

    case FLOAT:
      return new FloatFromStringGroupTreeReader(columnId, fileType);

    case DOUBLE:
      return new DoubleFromStringGroupTreeReader(columnId, fileType);

    case DECIMAL:
      return new DecimalFromStringGroupTreeReader(columnId, fileType, readerType);

    case STRING:
      return new StringGroupFromStringGroupTreeReader(columnId, fileType, readerType);

    case CHAR:
      return new StringGroupFromStringGroupTreeReader(columnId, fileType, readerType);

    case VARCHAR:
      return new StringGroupFromStringGroupTreeReader(columnId, fileType, readerType);

    case BINARY:
      return new BinaryFromStringGroupTreeReader(columnId, fileType);

    case TIMESTAMP:
      return new TimestampFromStringGroupTreeReader(columnId, fileType);

    case DATE:
      return new DateFromStringGroupTreeReader(columnId, fileType);

    // Not currently supported conversion(s):

    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new IllegalArgumentException("Unsupported type " +
          readerType.getCategory());
    }
  }

  private static TreeReader createTimestampConvertTreeReader(int columnId,
                                                             TypeDescription fileType,
                                                             TypeDescription readerType,
                                                             SchemaEvolution evolution,
                                                             boolean[] included,
                                                             boolean skipCorrupt) throws IOException {

    // CONVERT from TIMESTAMP to schema type.
    switch (readerType.getCategory()) {

    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return new AnyIntegerFromTimestampTreeReader(columnId, readerType, skipCorrupt);

    case FLOAT:
      return new FloatFromTimestampTreeReader(columnId, skipCorrupt);

    case DOUBLE:
      return new DoubleFromTimestampTreeReader(columnId, skipCorrupt);

    case DECIMAL:
      return new DecimalFromTimestampTreeReader(columnId, skipCorrupt);

    case STRING:
    case CHAR:
    case VARCHAR:
      return new StringGroupFromTimestampTreeReader(columnId, readerType, skipCorrupt);

    case TIMESTAMP:
      throw new IllegalArgumentException("No conversion of type " +
          readerType.getCategory() + " to self needed");

    case DATE:
      return new DateFromTimestampTreeReader(columnId, skipCorrupt);

    // Not currently supported conversion(s):
    case BINARY:

    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new IllegalArgumentException("Unsupported type " +
          readerType.getCategory());
    }
  }

  private static TreeReader createDateConvertTreeReader(int columnId,
                                                        TypeDescription fileType,
                                                        TypeDescription readerType,
                                                        SchemaEvolution evolution,
                                                        boolean[] included,
                                                        boolean skipCorrupt) throws IOException {

    // CONVERT from DATE to schema type.
    switch (readerType.getCategory()) {

    case STRING:
    case CHAR:
    case VARCHAR:
      return new StringGroupFromDateTreeReader(columnId, readerType, skipCorrupt);

    case TIMESTAMP:
      return new TimestampFromDateTreeReader(columnId, readerType, skipCorrupt);

    case DATE:
      throw new IllegalArgumentException("No conversion of type " +
          readerType.getCategory() + " to self needed");

      // Not currently supported conversion(s):
    case BOOLEAN:
    case BYTE:
    case FLOAT:
    case SHORT:
    case INT:
    case LONG:
    case DOUBLE:
    case BINARY:
    case DECIMAL:

    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new IllegalArgumentException("Unsupported type " +
          readerType.getCategory());
    }
  }

  private static TreeReader createBinaryConvertTreeReader(int columnId,
                                                          TypeDescription fileType,
                                                          TypeDescription readerType,
                                                          SchemaEvolution evolution,
                                                          boolean[] included,
                                                          boolean skipCorrupt) throws IOException {

    // CONVERT from DATE to schema type.
    switch (readerType.getCategory()) {

    case STRING:
    case CHAR:
    case VARCHAR:
      return new StringGroupFromBinaryTreeReader(columnId, readerType, skipCorrupt);

    case BINARY:
      throw new IllegalArgumentException("No conversion of type " +
          readerType.getCategory() + " to self needed");

      // Not currently supported conversion(s):
    case BOOLEAN:
    case BYTE:
    case FLOAT:
    case SHORT:
    case INT:
    case LONG:
    case DOUBLE:
    case TIMESTAMP:
    case DECIMAL:
    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new IllegalArgumentException("Unsupported type " +
          readerType.getCategory());
    }
  }

  /**
   * (Rules from Hive's PrimitiveObjectInspectorUtils conversion)
   *
   * To BOOLEAN, BYTE, SHORT, INT, LONG:
   *   Convert from (BOOLEAN, BYTE, SHORT, INT, LONG) with down cast if necessary.
   *   Convert from (FLOAT, DOUBLE) using type cast to long and down cast if necessary.
   *   Convert from DECIMAL from longValue and down cast if necessary.
   *   Convert from STRING using LazyLong.parseLong and down cast if necessary.
   *   Convert from (CHAR, VARCHAR) from Integer.parseLong and down cast if necessary.
   *   Convert from TIMESTAMP using timestamp getSeconds and down cast if necessary.
   *
   *   AnyIntegerFromAnyIntegerTreeReader (written)
   *   AnyIntegerFromFloatTreeReader (written)
   *   AnyIntegerFromDoubleTreeReader (written)
   *   AnyIntegerFromDecimalTreeReader (written)
   *   AnyIntegerFromStringGroupTreeReader (written)
   *   AnyIntegerFromTimestampTreeReader (written)
   *
   * To FLOAT/DOUBLE:
   *   Convert from (BOOLEAN, BYTE, SHORT, INT, LONG) using cast
   *   Convert from FLOAT using cast
   *   Convert from DECIMAL using getDouble
   *   Convert from (STRING, CHAR, VARCHAR) using Double.parseDouble
   *   Convert from TIMESTAMP using timestamp getDouble
   *
   *   FloatFromAnyIntegerTreeReader (existing)
   *   FloatFromDoubleTreeReader (written)
   *   FloatFromDecimalTreeReader (written)
   *   FloatFromStringGroupTreeReader (written)
   *
   *   DoubleFromAnyIntegerTreeReader (existing)
   *   DoubleFromFloatTreeReader (existing)
   *   DoubleFromDecimalTreeReader (written)
   *   DoubleFromStringGroupTreeReader (written)
   *
   * To DECIMAL:
   *   Convert from (BOOLEAN, BYTE, SHORT, INT, LONG) using to HiveDecimal.create()
   *   Convert from (FLOAT, DOUBLE) using to HiveDecimal.create(string value)
   *   Convert from (STRING, CHAR, VARCHAR) using HiveDecimal.create(string value)
   *   Convert from TIMESTAMP using HiveDecimal.create(string value of timestamp getDouble)
   *
   *   DecimalFromAnyIntegerTreeReader (existing)
   *   DecimalFromFloatTreeReader (existing)
   *   DecimalFromDoubleTreeReader (existing)
   *   DecimalFromStringGroupTreeReader (written)
   *
   * To STRING, CHAR, VARCHAR:
   *   Convert from (BOOLEAN, BYTE, SHORT, INT, LONG) using to string conversion
   *   Convert from (FLOAT, DOUBLE) using to string conversion
   *   Convert from DECIMAL using HiveDecimal.toString
   *   Convert from CHAR by stripping pads
   *   Convert from VARCHAR with value
   *   Convert from TIMESTAMP using Timestamp.toString
   *   Convert from DATE using Date.toString
   *   Convert from BINARY using Text.decode
   *
   *   StringGroupFromAnyIntegerTreeReader (written)
   *   StringGroupFromFloatTreeReader (written)
   *   StringGroupFromDoubleTreeReader (written)
   *   StringGroupFromDecimalTreeReader (written)
   *
   *   String from Char/Varchar conversion
   *   Char from String/Varchar conversion
   *   Varchar from String/Char conversion
   *
   *   StringGroupFromTimestampTreeReader (written)
   *   StringGroupFromDateTreeReader (written)
   *   StringGroupFromBinaryTreeReader *****
   *
   * To TIMESTAMP:
   *   Convert from (BOOLEAN, BYTE, SHORT, INT, LONG) using TimestampWritable.longToTimestamp
   *   Convert from (FLOAT, DOUBLE) using TimestampWritable.doubleToTimestamp
   *   Convert from DECIMAL using TimestampWritable.decimalToTimestamp
   *   Convert from (STRING, CHAR, VARCHAR) using string conversion
   *   Or, from DATE
   *
   *   TimestampFromAnyIntegerTreeReader (written)
   *   TimestampFromFloatTreeReader (written)
   *   TimestampFromDoubleTreeReader (written)
   *   TimestampFromDecimalTreeeReader (written)
   *   TimestampFromStringGroupTreeReader (written)
   *   TimestampFromDateTreeReader
   *
   *
   * To DATE:
   *   Convert from (STRING, CHAR, VARCHAR) using string conversion.
   *   Or, from TIMESTAMP.
   *
   *  DateFromStringGroupTreeReader (written)
   *  DateFromTimestampTreeReader (written)
   *
   * To BINARY:
   *   Convert from (STRING, CHAR, VARCHAR) using getBinaryFromText
   *
   *  BinaryFromStringGroupTreeReader (written)
   *
   * (Notes from StructConverter)
   *
   * To STRUCT:
   *   Input must be data type STRUCT
   *   minFields = Math.min(numSourceFields, numTargetFields)
   *   Convert those fields
   *   Extra targetFields --> NULL
   *
   * (Notes from ListConverter)
   *
   * To LIST:
   *   Input must be data type LIST
   *   Convert elements
   *
   * (Notes from MapConverter)
   *
   * To MAP:
   *   Input must be data type MAP
   *   Convert keys and values
   *
   * (Notes from UnionConverter)
   *
   * To UNION:
   *   Input must be data type UNION
   *   Convert value for tag
   *
   * @param readerType
   * @param evolution
   * @param included
   * @param skipCorrupt
   * @return
   * @throws IOException
   */
  public static TreeReader createConvertTreeReader(TypeDescription readerType,
                                                   SchemaEvolution evolution,
                                                   boolean[] included,
                                                   boolean skipCorrupt
                                                   ) throws IOException {

    int columnId = readerType.getId();
    TypeDescription fileType = evolution.getFileType(readerType);

    switch (fileType.getCategory()) {

    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return createAnyIntegerConvertTreeReader(columnId, fileType, readerType, evolution,
          included, skipCorrupt);

    case FLOAT:
      return createFloatConvertTreeReader(columnId, fileType, readerType, evolution,
          included, skipCorrupt);

    case DOUBLE:
      return createDoubleConvertTreeReader(columnId, fileType, readerType, evolution,
          included, skipCorrupt);

    case DECIMAL:
      return createDecimalConvertTreeReader(columnId, fileType, readerType, evolution,
          included, skipCorrupt);

    case STRING:
      return createStringConvertTreeReader(columnId, fileType, readerType, evolution,
          included, skipCorrupt);

    case CHAR:
      return createCharConvertTreeReader(columnId, fileType, readerType, evolution,
          included, skipCorrupt);

    case VARCHAR:
      return createVarcharConvertTreeReader(columnId, fileType, readerType, evolution,
          included, skipCorrupt);

    case TIMESTAMP:
      return createTimestampConvertTreeReader(columnId, fileType, readerType, evolution,
          included, skipCorrupt);

    case DATE:
      return createDateConvertTreeReader(columnId, fileType, readerType, evolution,
          included, skipCorrupt);

    case BINARY:
      return createBinaryConvertTreeReader(columnId, fileType, readerType, evolution,
          included, skipCorrupt);

    // UNDONE: Complex conversions...
    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new IllegalArgumentException("Unsupported type " +
          fileType.getCategory());
    }
  }

  public static boolean canConvert(TypeDescription fileType, TypeDescription readerType) {

    Category readerTypeCategory = readerType.getCategory();

    // We don't convert from any to complex.
    switch (readerTypeCategory) {
    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
      return false;

    default:
      // Fall through.
    }

    // Now look for the few cases we don't convert from
    switch (fileType.getCategory()) {

    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case DECIMAL:
      switch (readerType.getCategory()) {
      // Not currently supported conversion(s):
      case BINARY:
      case DATE:
        return false;
      default:
        return true;
      }


    case STRING:
    case CHAR:
    case VARCHAR:
      switch (readerType.getCategory()) {
      // Not currently supported conversion(s):
        // (None)
      default:
        return true;
      }

    case TIMESTAMP:
      switch (readerType.getCategory()) {
      // Not currently supported conversion(s):
      case BINARY:
        return false;
      default:
        return true;
      }

    case DATE:
      switch (readerType.getCategory()) {
      // Not currently supported conversion(s):
      case BOOLEAN:
      case BYTE:
      case FLOAT:
      case SHORT:
      case INT:
      case LONG:
      case DOUBLE:
      case BINARY:
      case DECIMAL:
        return false;
      default:
        return true;
      }

    case BINARY:
      switch (readerType.getCategory()) {
      // Not currently supported conversion(s):
      case BOOLEAN:
      case BYTE:
      case FLOAT:
      case SHORT:
      case INT:
      case LONG:
      case DOUBLE:
      case TIMESTAMP:
      case DECIMAL:
        return false;
      default:
        return true;
      }

    // We don't convert from complex to any.
    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
      return false;

    default:
      throw new IllegalArgumentException("Unsupported type " +
          fileType.getCategory());
    }
  }
}
