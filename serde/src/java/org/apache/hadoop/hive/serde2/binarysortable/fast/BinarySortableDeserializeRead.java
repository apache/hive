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

package org.apache.hadoop.hive.serde2.binarysortable.fast;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.InputByteBuffer;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Text;

/*
 * Directly deserialize with the caller reading field-by-field the LazyBinary serialization format.
 *
 * The caller is responsible for calling the read method for the right type of each field
 * (after calling readCheckNull).
 *
 * Reading some fields require a results object to receive value information.  A separate
 * results object is created by the caller at initialization per different field even for the same
 * type. 
 *
 * Some type values are by reference to either bytes in the deserialization buffer or to
 * other type specific buffers.  So, those references are only valid until the next time set is
 * called.
 */
public class BinarySortableDeserializeRead implements DeserializeRead {
  public static final Log LOG = LogFactory.getLog(BinarySortableDeserializeRead.class.getName());

  private PrimitiveTypeInfo[] primitiveTypeInfos;

  // The sort order (ascending/descending) for each field. Set to true when descending (invert).
  private boolean[] columnSortOrderIsDesc;

  // Which field we are on.  We start with -1 so readCheckNull can increment once and the read
  // field data methods don't increment.
  private int fieldIndex;

  private int fieldCount;

  private int start;

  private DecimalTypeInfo saveDecimalTypeInfo;
  private HiveDecimal saveDecimal;

  private byte[] tempDecimalBuffer;
  private HiveDecimalWritable tempHiveDecimalWritable;

  private boolean readBeyondConfiguredFieldsWarned;
  private boolean readBeyondBufferRangeWarned;
  private boolean bufferRangeHasExtraDataWarned;

  private InputByteBuffer inputByteBuffer = new InputByteBuffer();

  /*
   * Use this constructor when only ascending sort order is used.
   */
  public BinarySortableDeserializeRead(PrimitiveTypeInfo[] primitiveTypeInfos) {
    this(primitiveTypeInfos, null);
  }

  public BinarySortableDeserializeRead(PrimitiveTypeInfo[] primitiveTypeInfos,
          boolean[] columnSortOrderIsDesc) {
    this.primitiveTypeInfos = primitiveTypeInfos;
    fieldCount = primitiveTypeInfos.length;
    if (columnSortOrderIsDesc != null) {
      this.columnSortOrderIsDesc = columnSortOrderIsDesc;
    } else {
      this.columnSortOrderIsDesc = new boolean[primitiveTypeInfos.length];
      Arrays.fill(this.columnSortOrderIsDesc, false);
    }
    inputByteBuffer = new InputByteBuffer();
    readBeyondConfiguredFieldsWarned = false;
    readBeyondBufferRangeWarned = false;
    bufferRangeHasExtraDataWarned = false;
  }

  // Not public since we must have column information.
  private BinarySortableDeserializeRead() {
  }

  /*
   * The primitive type information for all fields.
   */
  public PrimitiveTypeInfo[] primitiveTypeInfos() {
    return primitiveTypeInfos;
  }

  /*
   * Set the range of bytes to be deserialized.
   */
  @Override
  public void set(byte[] bytes, int offset, int length) {
    fieldIndex = -1;
    inputByteBuffer.reset(bytes, offset, offset + length);
    start = offset;
  }

  /*
   * Reads the NULL information for a field.
   *
   * @return Returns true when the field is NULL; reading is positioned to the next field.
   *         Otherwise, false when the field is NOT NULL; reading is positioned to the field data.
   */
  @Override
  public boolean readCheckNull() throws IOException {

    // We start with fieldIndex as -1 so we can increment once here and then the read
    // field data methods don't increment.
    fieldIndex++;

    if (fieldIndex >= fieldCount) {
      // Reading beyond the specified field count produces NULL.
      if (!readBeyondConfiguredFieldsWarned) {
        // Warn only once.
        LOG.info("Reading beyond configured fields! Configured " + fieldCount + " fields but "
            + " reading more (NULLs returned).  Ignoring similar problems.");
        readBeyondConfiguredFieldsWarned = true;
      }
      return true;
    }
    if (inputByteBuffer.isEof()) {
      // Also, reading beyond our byte range produces NULL.
      if (!readBeyondBufferRangeWarned) {
        // Warn only once.
        int length = inputByteBuffer.tell() - start;
        LOG.info("Reading beyond buffer range! Buffer range " +  start 
            + " for length " + length + " but reading more... "
            + "(total buffer length " + inputByteBuffer.getData().length + ")"
            + "  Ignoring similar problems.");
        readBeyondBufferRangeWarned = true;
      }
      // We cannot read beyond so we must return NULL here.
      return true;
    }
    byte isNull = inputByteBuffer.read(columnSortOrderIsDesc[fieldIndex]);

    if (isNull == 0) {
      return true;
    }

    // We have a field and are positioned to it.

    if (primitiveTypeInfos[fieldIndex].getPrimitiveCategory() != PrimitiveCategory.DECIMAL) {
      return false;
    }

    // Since enforcing precision and scale may turn a HiveDecimal into a NULL, we must read
    // it here.
    return earlyReadHiveDecimal();
  }

  /*
   * Call this method after all fields have been read to check for extra fields.
   */
  public void extraFieldsCheck() {
    if (!inputByteBuffer.isEof()) {
      // We did not consume all of the byte range.
      if (!bufferRangeHasExtraDataWarned) {
        // Warn only once.
       int length = inputByteBuffer.getEnd() - start;
       int remaining = inputByteBuffer.getEnd() - inputByteBuffer.tell();
        LOG.info("Not all fields were read in the buffer range! Buffer range " +  start 
            + " for length " + length + " but " + remaining + " bytes remain. "
            + "(total buffer length " + inputByteBuffer.getData().length + ")"
            + "  Ignoring similar problems.");
        bufferRangeHasExtraDataWarned = true;
      }
    }
  }

  /*
   * Read integrity warning flags.
   */
  @Override
  public boolean readBeyondConfiguredFieldsWarned() {
    return readBeyondConfiguredFieldsWarned;
  }
  @Override
  public boolean readBeyondBufferRangeWarned() {
    return readBeyondBufferRangeWarned;
  }
  @Override
  public boolean bufferRangeHasExtraDataWarned() {
    return bufferRangeHasExtraDataWarned;
  }

  /*
   * BOOLEAN.
   */
  @Override
  public boolean readBoolean() throws IOException {
    byte b = inputByteBuffer.read(columnSortOrderIsDesc[fieldIndex]);
    return (b == 2);
  }

  /*
   * BYTE.
   */
  @Override
  public byte readByte() throws IOException {
    return (byte) (inputByteBuffer.read(columnSortOrderIsDesc[fieldIndex]) ^ 0x80);
  }

  /*
   * SHORT.
   */
  @Override
  public short readShort() throws IOException {
    final boolean invert = columnSortOrderIsDesc[fieldIndex];
    int v = inputByteBuffer.read(invert) ^ 0x80;
    v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
    return (short) v;
  }

  /*
   * INT.
   */
  @Override
  public int readInt() throws IOException {
    final boolean invert = columnSortOrderIsDesc[fieldIndex];
    int v = inputByteBuffer.read(invert) ^ 0x80;
    for (int i = 0; i < 3; i++) {
      v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
    }
    return v;
  }

  /*
   * LONG.
   */
  @Override
  public long readLong() throws IOException {
    final boolean invert = columnSortOrderIsDesc[fieldIndex];
    long v = inputByteBuffer.read(invert) ^ 0x80;
    for (int i = 0; i < 7; i++) {
      v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
    }
    return v;
  }

  /*
   * FLOAT.
   */
  @Override
  public float readFloat() throws IOException {
    final boolean invert = columnSortOrderIsDesc[fieldIndex];
    int v = 0;
    for (int i = 0; i < 4; i++) {
      v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
    }
    if ((v & (1 << 31)) == 0) {
      // negative number, flip all bits
      v = ~v;
    } else {
      // positive number, flip the first bit
      v = v ^ (1 << 31);
    }
    return Float.intBitsToFloat(v);
  }

  /*
   * DOUBLE.
   */
  @Override
  public double readDouble() throws IOException {
    final boolean invert = columnSortOrderIsDesc[fieldIndex];
    long v = 0;
    for (int i = 0; i < 8; i++) {
      v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
    }
    if ((v & (1L << 63)) == 0) {
      // negative number, flip all bits
      v = ~v;
    } else {
      // positive number, flip the first bit
      v = v ^ (1L << 63);
    }
    return Double.longBitsToDouble(v);
  }

  // This class is for internal use.
  private static class BinarySortableReadStringResults extends ReadStringResults {

    // Use an org.apache.hadoop.io.Text object as a buffer to decode the BinarySortable
    // format string into.
    private Text text;

    public BinarySortableReadStringResults() {
      super();
      text = new Text();
    }
  }

  // Reading a STRING field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different bytes field. 
  @Override
  public ReadStringResults createReadStringResults() {
    return new BinarySortableReadStringResults();
  }
  

  @Override
  public void readString(ReadStringResults readStringResults) throws IOException {
    BinarySortableReadStringResults binarySortableReadStringResults = 
            (BinarySortableReadStringResults) readStringResults;

    BinarySortableSerDe.deserializeText(inputByteBuffer, columnSortOrderIsDesc[fieldIndex], binarySortableReadStringResults.text);
    readStringResults.bytes = binarySortableReadStringResults.text.getBytes();
    readStringResults.start = 0;
    readStringResults.length = binarySortableReadStringResults.text.getLength();
  }


  /*
   * CHAR.
   */

  // This class is for internal use.
  private static class BinarySortableReadHiveCharResults extends ReadHiveCharResults {

    public BinarySortableReadHiveCharResults() {
      super();
    }

    public HiveCharWritable getHiveCharWritable() {
      return hiveCharWritable;
    }
  }

  // Reading a CHAR field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different CHAR field. 
  @Override
  public ReadHiveCharResults createReadHiveCharResults() {
    return new BinarySortableReadHiveCharResults();
  }

  public void readHiveChar(ReadHiveCharResults readHiveCharResults) throws IOException {
    BinarySortableReadHiveCharResults binarySortableReadHiveCharResults = 
            (BinarySortableReadHiveCharResults) readHiveCharResults;

    if (!binarySortableReadHiveCharResults.isInit()) {
      binarySortableReadHiveCharResults.init((CharTypeInfo) primitiveTypeInfos[fieldIndex]);
    }

    HiveCharWritable hiveCharWritable = binarySortableReadHiveCharResults.getHiveCharWritable();

    // Decode the bytes into our Text buffer, then truncate.
    BinarySortableSerDe.deserializeText(inputByteBuffer, columnSortOrderIsDesc[fieldIndex], hiveCharWritable.getTextValue());
    hiveCharWritable.enforceMaxLength(binarySortableReadHiveCharResults.getMaxLength());

    readHiveCharResults.bytes = hiveCharWritable.getTextValue().getBytes();
    readHiveCharResults.start = 0;
    readHiveCharResults.length = hiveCharWritable.getTextValue().getLength();
  }

  /*
   * VARCHAR.
   */

  // This class is for internal use.
  private static class BinarySortableReadHiveVarcharResults extends ReadHiveVarcharResults {

    public BinarySortableReadHiveVarcharResults() {
      super();
    }

    public HiveVarcharWritable getHiveVarcharWritable() {
      return hiveVarcharWritable;
    }
  }

  // Reading a VARCHAR field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different VARCHAR field. 
  @Override
  public ReadHiveVarcharResults createReadHiveVarcharResults() {
    return new BinarySortableReadHiveVarcharResults();
  }

  public void readHiveVarchar(ReadHiveVarcharResults readHiveVarcharResults) throws IOException {
    BinarySortableReadHiveVarcharResults binarySortableReadHiveVarcharResults = (BinarySortableReadHiveVarcharResults) readHiveVarcharResults;

    if (!binarySortableReadHiveVarcharResults.isInit()) {
      binarySortableReadHiveVarcharResults.init((VarcharTypeInfo) primitiveTypeInfos[fieldIndex]);
    }

    HiveVarcharWritable hiveVarcharWritable = binarySortableReadHiveVarcharResults.getHiveVarcharWritable();

    // Decode the bytes into our Text buffer, then truncate.
    BinarySortableSerDe.deserializeText(inputByteBuffer, columnSortOrderIsDesc[fieldIndex], hiveVarcharWritable.getTextValue());
    hiveVarcharWritable.enforceMaxLength(binarySortableReadHiveVarcharResults.getMaxLength());

    readHiveVarcharResults.bytes = hiveVarcharWritable.getTextValue().getBytes();
    readHiveVarcharResults.start = 0;
    readHiveVarcharResults.length = hiveVarcharWritable.getTextValue().getLength();
  }

  /*
   * BINARY.
   */

  // This class is for internal use.
  private static class BinarySortableReadBinaryResults extends ReadBinaryResults {

    // Use an org.apache.hadoop.io.Text object as a buffer to decode the BinarySortable
    // format string into.
    private Text text;

    public BinarySortableReadBinaryResults() {
      super();
      text = new Text();
    }
  }

  // Reading a BINARY field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different bytes field. 
  @Override
  public ReadBinaryResults createReadBinaryResults() {
    return new BinarySortableReadBinaryResults();
  }

  @Override
  public void readBinary(ReadBinaryResults readBinaryResults) throws IOException {
    BinarySortableReadBinaryResults binarySortableReadBinaryResults = 
            (BinarySortableReadBinaryResults) readBinaryResults;

    BinarySortableSerDe.deserializeText(inputByteBuffer, columnSortOrderIsDesc[fieldIndex], binarySortableReadBinaryResults.text);
    readBinaryResults.bytes = binarySortableReadBinaryResults.text.getBytes();
    readBinaryResults.start = 0;
    readBinaryResults.length = binarySortableReadBinaryResults.text.getLength();
  }

  /*
   * DATE.
   */

  // This class is for internal use.
  private static class BinarySortableReadDateResults extends ReadDateResults {

    public BinarySortableReadDateResults() {
      super();
    }

    public DateWritable getDateWritable() {
      return dateWritable;
    }
  }

  // Reading a DATE field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different DATE field. 
  @Override
  public ReadDateResults createReadDateResults() {
    return new BinarySortableReadDateResults();
  }

  @Override
  public void readDate(ReadDateResults readDateResults) throws IOException {
    BinarySortableReadDateResults binarySortableReadDateResults = (BinarySortableReadDateResults) readDateResults;
    final boolean invert = columnSortOrderIsDesc[fieldIndex];
    int v = inputByteBuffer.read(invert) ^ 0x80;
    for (int i = 0; i < 3; i++) {
      v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
    }
    DateWritable dateWritable = binarySortableReadDateResults.getDateWritable();
    dateWritable.set(v);
  }

  /*
   * TIMESTAMP.
   */

  // This class is for internal use.
  private static class BinarySortableReadTimestampResults extends ReadTimestampResults {

    private byte[] timestampBytes;

    public BinarySortableReadTimestampResults() {
      super();
      timestampBytes = new byte[TimestampWritable.BINARY_SORTABLE_LENGTH];
    }

    public TimestampWritable getTimestampWritable() {
      return timestampWritable;
    }
  }

  // Reading a TIMESTAMP field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different TIMESTAMP field. 
  @Override
  public ReadTimestampResults createReadTimestampResults() {
    return new BinarySortableReadTimestampResults();
  }

  @Override
  public void readTimestamp(ReadTimestampResults readTimestampResults) throws IOException {
    BinarySortableReadTimestampResults binarySortableReadTimestampResults = (BinarySortableReadTimestampResults) readTimestampResults;
    final boolean invert = columnSortOrderIsDesc[fieldIndex];
    byte[] timestampBytes = binarySortableReadTimestampResults.timestampBytes;
    for (int i = 0; i < timestampBytes.length; i++) {
      timestampBytes[i] = inputByteBuffer.read(invert);
    }
    TimestampWritable timestampWritable = binarySortableReadTimestampResults.getTimestampWritable();
    timestampWritable.setBinarySortable(timestampBytes, 0);
  }

  /*
   * INTERVAL_YEAR_MONTH.
   */

  // This class is for internal use.
  private static class BinarySortableReadIntervalYearMonthResults extends ReadIntervalYearMonthResults {

    public BinarySortableReadIntervalYearMonthResults() {
      super();
    }

    public HiveIntervalYearMonthWritable getHiveIntervalYearMonthWritable() {
      return hiveIntervalYearMonthWritable;
    }
  }

  // Reading a INTERVAL_YEAR_MONTH field require a results object to receive value information.
  // A separate results object is created by the caller at initialization per different
  // INTERVAL_YEAR_MONTH field. 
  @Override
  public ReadIntervalYearMonthResults createReadIntervalYearMonthResults() {
    return new BinarySortableReadIntervalYearMonthResults();
  }

  @Override
  public void readIntervalYearMonth(ReadIntervalYearMonthResults readIntervalYearMonthResults)
          throws IOException {
    BinarySortableReadIntervalYearMonthResults binarySortableReadIntervalYearMonthResults =
                (BinarySortableReadIntervalYearMonthResults) readIntervalYearMonthResults;
    final boolean invert = columnSortOrderIsDesc[fieldIndex];
    int v = inputByteBuffer.read(invert) ^ 0x80;
    for (int i = 0; i < 3; i++) {
      v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
    }
    HiveIntervalYearMonthWritable hiveIntervalYearMonthWritable = 
                binarySortableReadIntervalYearMonthResults.getHiveIntervalYearMonthWritable();
    hiveIntervalYearMonthWritable.set(v);
  }

  /*
   * INTERVAL_DAY_TIME.
   */

  // This class is for internal use.
  private static class BinarySortableReadIntervalDayTimeResults extends ReadIntervalDayTimeResults {

    public BinarySortableReadIntervalDayTimeResults() {
      super();
    }

    public HiveIntervalDayTimeWritable getHiveIntervalDayTimeWritable() {
      return hiveIntervalDayTimeWritable;
    }
  }

  // Reading a INTERVAL_DAY_TIME field require a results object to receive value information.
  // A separate results object is created by the caller at initialization per different
  // INTERVAL_DAY_TIME field. 
  @Override
  public ReadIntervalDayTimeResults createReadIntervalDayTimeResults() {
    return new BinarySortableReadIntervalDayTimeResults();
  }

  @Override
  public void readIntervalDayTime(ReadIntervalDayTimeResults readIntervalDayTimeResults)
          throws IOException {
    BinarySortableReadIntervalDayTimeResults binarySortableReadIntervalDayTimeResults =
                (BinarySortableReadIntervalDayTimeResults) readIntervalDayTimeResults;
    final boolean invert = columnSortOrderIsDesc[fieldIndex];
    long totalSecs = inputByteBuffer.read(invert) ^ 0x80;
    for (int i = 0; i < 7; i++) {
      totalSecs = (totalSecs << 8) + (inputByteBuffer.read(invert) & 0xff);
    }
    int nanos = inputByteBuffer.read(invert) ^ 0x80;
    for (int i = 0; i < 3; i++) {
      nanos = (nanos << 8) + (inputByteBuffer.read(invert) & 0xff);
    }
    HiveIntervalDayTimeWritable hiveIntervalDayTimeWritable = 
                binarySortableReadIntervalDayTimeResults.getHiveIntervalDayTimeWritable();
    hiveIntervalDayTimeWritable.set(totalSecs, nanos);
  }

  /*
   * DECIMAL.
   */

  // This class is for internal use.
  private static class BinarySortableReadDecimalResults extends ReadDecimalResults {

    public HiveDecimal hiveDecimal;

    public BinarySortableReadDecimalResults() {
      super();
    }

    @Override
    public void init(DecimalTypeInfo decimalTypeInfo) {
      super.init(decimalTypeInfo);
    }

    @Override
    public HiveDecimal getHiveDecimal() {
      return hiveDecimal;
    }
  }

  // Reading a DECIMAL field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different DECIMAL field. 
  @Override
  public ReadDecimalResults createReadDecimalResults() {
    return new BinarySortableReadDecimalResults();
  }

  @Override
  public void readHiveDecimal(ReadDecimalResults readDecimalResults) throws IOException {
    BinarySortableReadDecimalResults binarySortableReadDecimalResults = 
            (BinarySortableReadDecimalResults) readDecimalResults;

    if (!binarySortableReadDecimalResults.isInit()) {
      binarySortableReadDecimalResults.init(saveDecimalTypeInfo);
    }

    binarySortableReadDecimalResults.hiveDecimal = saveDecimal;

    saveDecimal = null;
    saveDecimalTypeInfo = null;
  }

  /**
   * We read the whole HiveDecimal value and then enforce precision and scale, which may
   * make it a NULL.
   * @return     Returns true if this HiveDecimal enforced to a NULL.
   * @throws IOException 
   */
  private boolean earlyReadHiveDecimal() throws IOException {

    // Since enforcing precision and scale can cause a HiveDecimal to become NULL,
    // we must read it, enforce it here, and either return NULL or buffer the result.

    final boolean invert = columnSortOrderIsDesc[fieldIndex];
    int b = inputByteBuffer.read(invert) - 1;
    assert (b == 1 || b == -1 || b == 0);
    boolean positive = b != -1;

    int factor = inputByteBuffer.read(invert) ^ 0x80;
    for (int i = 0; i < 3; i++) {
      factor = (factor << 8) + (inputByteBuffer.read(invert) & 0xff);
    }

    if (!positive) {
      factor = -factor;
    }

    int start = inputByteBuffer.tell();
    int length = 0;

    do {
      b = inputByteBuffer.read(positive ? invert : !invert);
      assert(b != 1);

      if (b == 0) {
        // end of digits
        break;
      }

      length++;
    } while (true);

    if(tempDecimalBuffer == null || tempDecimalBuffer.length < length) {
      tempDecimalBuffer = new byte[length];
    }

    inputByteBuffer.seek(start);
    for (int i = 0; i < length; ++i) {
      tempDecimalBuffer[i] = inputByteBuffer.read(positive ? invert : !invert);
    }

    // read the null byte again
    inputByteBuffer.read(positive ? invert : !invert);

    String digits = new String(tempDecimalBuffer, 0, length, BinarySortableSerDe.decimalCharSet);
    BigInteger bi = new BigInteger(digits);
    HiveDecimal bd = HiveDecimal.create(bi).scaleByPowerOfTen(factor-length);

    if (!positive) {
      bd = bd.negate();
    }

    // We have a decimal.  After we enforce precision and scale, will it become a NULL?

    if (tempHiveDecimalWritable == null) {
      tempHiveDecimalWritable = new HiveDecimalWritable();
    }
    tempHiveDecimalWritable.set(bd);

    saveDecimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfos[fieldIndex];

    int precision = saveDecimalTypeInfo.getPrecision();
    int scale = saveDecimalTypeInfo.getScale();

    saveDecimal = tempHiveDecimalWritable.getHiveDecimal(precision, scale);

    // Now return whether it is NULL or NOT NULL.
    return (saveDecimal == null);
  }
}