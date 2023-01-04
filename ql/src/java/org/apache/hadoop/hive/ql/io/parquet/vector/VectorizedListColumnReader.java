/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * It's column level Parquet reader which is used to read a batch of records for a list column.
 * TODO Currently List type only support non nested case.
 */
public class VectorizedListColumnReader extends BaseVectorizedColumnReader {

  // The value read in last time
  private Object lastValue;

  // flag to indicate if there is no data in parquet data page
  private boolean eof = false;

  // flag to indicate if it's the first time to read parquet data page with this instance
  boolean isFirstRow = true;

  public VectorizedListColumnReader(ColumnDescriptor descriptor, PageReader pageReader,
      boolean skipTimestampConversion, ZoneId writerTimezone, boolean skipProlepticConversion,
      boolean legacyConversionEnabled, Type type, TypeInfo hiveType) throws IOException {
    super(descriptor, pageReader, skipTimestampConversion, writerTimezone, skipProlepticConversion,
        legacyConversionEnabled, type, hiveType);
  }

  @Override
  public void readBatch(int total, ColumnVector column, TypeInfo columnType) throws IOException {
    ListColumnVector lcv = (ListColumnVector) column;
    // before readBatch, initial the size of offsets & lengths as the default value,
    // the actual size will be assigned in setChildrenInfo() after reading complete.
    lcv.offsets = new long[VectorizedRowBatch.DEFAULT_SIZE];
    lcv.lengths = new long[VectorizedRowBatch.DEFAULT_SIZE];
    // Because the length of ListColumnVector.child can't be known now,
    // the valueList will save all data for ListColumnVector temporary.
    List<Object> valueList = new ArrayList<>();

    PrimitiveObjectInspector.PrimitiveCategory category =
        ((PrimitiveTypeInfo) ((ListTypeInfo) columnType).getListElementTypeInfo()).getPrimitiveCategory();

    // read the first row in parquet data page, this will be only happened once for this instance
    if(isFirstRow){
      if (!fetchNextValue(category)) {
        return;
      }
      isFirstRow = false;
    }

    int index = collectDataFromParquetPage(total, lcv, valueList, category);

    // Convert valueList to array for the ListColumnVector.child
    convertValueListToListColumnVector(category, lcv, valueList, index);
  }

  /**
   * Collects data from a parquet page and returns the final row index where it stopped.
   * The returned index can be equal to or less than total.
   * @param total maximum number of rows to collect
   * @param lcv column vector to do initial setup in data collection time
   * @param valueList collection of values that will be fed into the vector later
   * @param category
   * @return
   * @throws IOException
   */
  private int collectDataFromParquetPage(int total, ListColumnVector lcv, List<Object> valueList,
      PrimitiveObjectInspector.PrimitiveCategory category) throws IOException {
    int index = 0;
    /*
     * Here is a nested loop for collecting all values from a parquet page.
     * A column of array type can be considered as a list of lists, so the two loops are as below:
     * 1. The outer loop iterates on rows (index is a row index, so points to a row in the batch), e.g.:
     * [0, 2, 3]    <- index: 0
     * [NULL, 3, 4] <- index: 1
     *
     * 2. The inner loop iterates on values within a row (sets all data from parquet data page
     * for an element in ListColumnVector), so fetchNextValue returns values one-by-one:
     * 0, 2, 3, NULL, 3, 4
     *
     * As described below, the repetition level (repetitionLevel != 0)
     * can be used to decide when we'll start to read values for the next list.
     */
    while (!eof && index < total) {
      // add element to ListColumnVector one by one
      lcv.offsets[index] = valueList.size();
      /*
       * Let's collect all values for a single list.
       * Repetition level = 0 means that a new list started there in the parquet page,
       * in that case, let's exit from the loop, and start to collect value for a new list.
       */
      do {
        /*
         * Definition level = 0 when a NULL value was returned instead of a list
         * (this is not the same as a NULL value in of a list).
         */
        if (definitionLevel == 0) {
          lcv.isNull[index] = true;
          lcv.noNulls = false;
        }
        valueList
            .add(isCurrentPageDictionaryEncoded ? dictionaryDecodeValue(category, (Integer) lastValue) : lastValue);
      } while (fetchNextValue(category) && (repetitionLevel != 0));

      lcv.lengths[index] = valueList.size() - lcv.offsets[index];
      index++;
    }
    return index;
  }

  private int readPageIfNeed() throws IOException {
    // Compute the number of values we want to read in this page.
    int leftInPage = (int) (endOfPageValueCount - valuesRead);
    if (leftInPage == 0) {
      // no data left in current page, load data from new page
      readPage();
      leftInPage = (int) (endOfPageValueCount - valuesRead);
    }
    return leftInPage;
  }

  /**
   * Reads a single value from parquet page, puts it into lastValue.
   * Returns a boolean indicating if there is more values to read (true).
   * @param category
   * @return
   * @throws IOException
   */
  private boolean fetchNextValue(PrimitiveObjectInspector.PrimitiveCategory category) throws IOException {
    int left = readPageIfNeed();
    if (left > 0) {
      // get the values of repetition and definitionLevel
      readRepetitionAndDefinitionLevels();
      // read the data if it isn't null
      if (definitionLevel == maxDefLevel) {
        if (isCurrentPageDictionaryEncoded) {
          lastValue = dataColumn.readValueDictionaryId();
        } else {
          lastValue = readPrimitiveTypedRow(category);
        }
      } else {
        lastValue = null;
      }
      return true;
    } else {
      eof = true;
      return false;
    }
  }

  // Need to be in consistent with that VectorizedPrimitiveColumnReader#readBatchHelper
  // TODO Reduce the duplicated code
  private Object readPrimitiveTypedRow(PrimitiveObjectInspector.PrimitiveCategory category) {
    switch (category) {
    case INT:
    case BYTE:
    case SHORT:
      return dataColumn.readInteger();
    case DATE:
    case INTERVAL_YEAR_MONTH:
    case LONG:
      return dataColumn.readLong();
    case BOOLEAN:
      return dataColumn.readBoolean() ? 1 : 0;
    case DOUBLE:
      return dataColumn.readDouble();
    case BINARY:
      return dataColumn.readBytes();
    case STRING:
    case CHAR:
    case VARCHAR:
      return dataColumn.readString();
    case FLOAT:
      return dataColumn.readFloat();
    case DECIMAL:
      return dataColumn.readDecimal();
    case TIMESTAMP:
      return dataColumn.readTimestamp();
    case INTERVAL_DAY_TIME:
    default:
      throw new RuntimeException("Unsupported type in the list: " + type);
    }
  }

  private Object dictionaryDecodeValue(PrimitiveObjectInspector.PrimitiveCategory category, Integer dictionaryValue) {
    if (dictionaryValue == null) {
      return null;
    }

    switch (category) {
    case INT:
    case BYTE:
    case SHORT:
      return dictionary.readInteger(dictionaryValue);
    case DATE:
    case INTERVAL_YEAR_MONTH:
    case LONG:
      return dictionary.readLong(dictionaryValue);
    case BOOLEAN:
      return dictionary.readBoolean(dictionaryValue) ? 1 : 0;
    case DOUBLE:
      return dictionary.readDouble(dictionaryValue);
    case BINARY:
      return dictionary.readBytes(dictionaryValue);
    case STRING:
    case CHAR:
    case VARCHAR:
      return dictionary.readString(dictionaryValue);
    case FLOAT:
      return dictionary.readFloat(dictionaryValue);
    case DECIMAL:
      return dictionary.readDecimal(dictionaryValue);
    case TIMESTAMP:
      return dictionary.readTimestamp(dictionaryValue);
    case INTERVAL_DAY_TIME:
    default:
      throw new RuntimeException("Unsupported type in the list: " + type);
    }
  }

  /**
   * The lengths & offsets will be initialized as default size (1024),
   * it should be set to the actual size according to the element number.
   */
  private void setChildrenInfo(ListColumnVector lcv, int itemNum, int elementNum) {
    lcv.childCount = itemNum;
    long[] lcvLength = new long[elementNum];
    long[] lcvOffset = new long[elementNum];
    System.arraycopy(lcv.lengths, 0, lcvLength, 0, elementNum);
    System.arraycopy(lcv.offsets, 0, lcvOffset, 0, elementNum);
    lcv.lengths = lcvLength;
    lcv.offsets = lcvOffset;
  }

  private void fillColumnVector(PrimitiveObjectInspector.PrimitiveCategory category,
                                ListColumnVector lcv,
                                List valueList, int elementNum) {
    int total = valueList.size();
    setChildrenInfo(lcv, total, elementNum);
    switch (category) {
    case BOOLEAN:
      lcv.child = new LongColumnVector(total);
      for (int i = 0; i < valueList.size(); i++) {
        if (valueList.get(i) == null) {
          lcv.child.isNull[i] = true;
        } else {
          ((LongColumnVector) lcv.child).vector[i] = ((List<Integer>) valueList).get(i);
        }
      }
      break;
    case INT:
    case BYTE:
    case SHORT:
    case DATE:
    case INTERVAL_YEAR_MONTH:
    case LONG:
      lcv.child = new LongColumnVector(total);
      for (int i = 0; i < valueList.size(); i++) {
        if (valueList.get(i) == null) {
          lcv.child.isNull[i] = true;
        } else {
          ((LongColumnVector) lcv.child).vector[i] = ((List<Long>) valueList).get(i);
        }
      }
      break;
    case DOUBLE:
      lcv.child = new DoubleColumnVector(total);
      for (int i = 0; i < valueList.size(); i++) {
        if (valueList.get(i) == null) {
          lcv.child.isNull[i] = true;
        } else {
          ((DoubleColumnVector) lcv.child).vector[i] = ((List<Double>) valueList).get(i);
        }
      }
      break;
    case BINARY:
    case STRING:
    case CHAR:
    case VARCHAR:
      lcv.child = new BytesColumnVector(total);
      lcv.child.init();
      for (int i = 0; i < valueList.size(); i++) {
        byte[] src = ((List<byte[]>) valueList).get(i);
        if (src == null) {
          ((BytesColumnVector) lcv.child).setRef(i, src, 0, 0);
          lcv.child.isNull[i] = true;
        } else {
          ((BytesColumnVector) lcv.child).setRef(i, src, 0, src.length);
        }
      }
      break;
    case FLOAT:
      lcv.child = new DoubleColumnVector(total);
      for (int i = 0; i < valueList.size(); i++) {
        if (valueList.get(i) == null) {
          lcv.child.isNull[i] = true;
        } else {
          ((DoubleColumnVector) lcv.child).vector[i] = ((List<Float>) valueList).get(i);
        }
      }
      break;
    case DECIMAL:
      decimalTypeCheck(type);
      DecimalLogicalTypeAnnotation logicalType = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
      int precision = logicalType.getPrecision();
      int scale = logicalType.getScale();
      lcv.child = new DecimalColumnVector(total, precision, scale);
      for (int i = 0; i < valueList.size(); i++) {
        if (valueList.get(i) == null) {
          lcv.child.isNull[i] = true;
        } else {
          ((DecimalColumnVector) lcv.child).vector[i].set(((List<byte[]>) valueList).get(i), scale);
        }
      }
      break;
    case INTERVAL_DAY_TIME:
    case TIMESTAMP:
    default:
      throw new RuntimeException("Unsupported type in the list: " + type);
    }
  }

  /**
   * Finish the result ListColumnVector with all collected information.
   */
  private void convertValueListToListColumnVector(
      PrimitiveObjectInspector.PrimitiveCategory category, ListColumnVector lcv, List valueList,
      int elementNum) {
    // Fill the child of ListColumnVector with valueList
    fillColumnVector(category, lcv, valueList, elementNum);
    setIsRepeating(lcv);
  }

  private void setIsRepeating(ListColumnVector lcv) {
    ColumnVector child0 = getChildData(lcv, 0);
    for (int i = 1; i < lcv.offsets.length; i++) {
      ColumnVector currentChild = getChildData(lcv, i);
      if (!compareColumnVector(child0, currentChild)) {
        lcv.isRepeating = false;
        return;
      }
    }
    lcv.isRepeating = true;
  }

  /**
   * Get the child ColumnVector of ListColumnVector
   */
  private ColumnVector getChildData(ListColumnVector lcv, int index) {
    if (lcv.offsets[index] > Integer.MAX_VALUE || lcv.lengths[index] > Integer.MAX_VALUE) {
      throw new RuntimeException("The element number in list is out of scope.");
    }
    if (lcv.isNull[index]) {
      return null;
    }
    int start = (int)lcv.offsets[index];
    int length = (int)lcv.lengths[index];
    ColumnVector child = lcv.child;
    ColumnVector resultCV = null;
    if (child instanceof LongColumnVector) {
      resultCV = new LongColumnVector(length);
      try {
        System.arraycopy(((LongColumnVector) lcv.child).vector, start,
            ((LongColumnVector) resultCV).vector, 0, length);
      } catch (Exception e) {
        throw new RuntimeException(
            "Fail to copy at index:" + index + ", start:" + start + ",length:" + length + ",vec " +
                "len:" + ((LongColumnVector) lcv.child).vector.length + ", offset len:" + lcv
                .offsets.length + ", len len:" + lcv.lengths.length, e);
      }
    }
    if (child instanceof DoubleColumnVector) {
      resultCV = new DoubleColumnVector(length);
      System.arraycopy(((DoubleColumnVector) lcv.child).vector, start,
          ((DoubleColumnVector) resultCV).vector, 0, length);
    }
    if (child instanceof BytesColumnVector) {
      resultCV = new BytesColumnVector(length);
      System.arraycopy(((BytesColumnVector) lcv.child).vector, start,
          ((BytesColumnVector) resultCV).vector, 0, length);
    }
    if (child instanceof DecimalColumnVector) {
      resultCV = new DecimalColumnVector(length,
          ((DecimalColumnVector) child).precision, ((DecimalColumnVector) child).scale);
      System.arraycopy(((DecimalColumnVector) lcv.child).vector, start,
          ((DecimalColumnVector) resultCV).vector, 0, length);
    }
    System.arraycopy(lcv.child.isNull, start, resultCV.isNull, 0, length);
    return resultCV;
  }

  private boolean compareColumnVector(ColumnVector cv1, ColumnVector cv2) {
    if (cv1 == null && cv2 == null) {
      return true;
    } else {
      if (cv1 != null && cv2 != null) {
        if (cv1 instanceof LongColumnVector && cv2 instanceof LongColumnVector) {
          return compareLongColumnVector((LongColumnVector) cv1, (LongColumnVector) cv2);
        }
        if (cv1 instanceof DoubleColumnVector && cv2 instanceof DoubleColumnVector) {
          return compareDoubleColumnVector((DoubleColumnVector) cv1, (DoubleColumnVector) cv2);
        }
        if (cv1 instanceof BytesColumnVector && cv2 instanceof BytesColumnVector) {
          return compareBytesColumnVector((BytesColumnVector) cv1, (BytesColumnVector) cv2);
        }
        if (cv1 instanceof DecimalColumnVector && cv2 instanceof DecimalColumnVector) {
          return compareDecimalColumnVector((DecimalColumnVector) cv1, (DecimalColumnVector) cv2);
        }
        throw new RuntimeException(
            "Unsupported ColumnVector comparision between " + cv1.getClass().getName()
                + " and " + cv2.getClass().getName());
      } else {
        return false;
      }
    }
  }

  private boolean compareLongColumnVector(LongColumnVector cv1, LongColumnVector cv2) {
    int length1 = cv1.vector.length;
    int length2 = cv2.vector.length;
    if (length1 == length2) {
      for (int i = 0; i < length1; i++) {
        if (columnVectorsDifferNullForSameIndex(cv1, cv2, i)) {
          return false;
        }
        if (!cv1.isNull[i] && !cv2.isNull[i] && cv1.vector[i] != cv2.vector[i]) {
          return false;
        }
      }
    } else {
      return false;
    }
    return true;
  }

  private boolean compareDoubleColumnVector(DoubleColumnVector cv1, DoubleColumnVector cv2) {
    int length1 = cv1.vector.length;
    int length2 = cv2.vector.length;
    if (length1 == length2) {
      for (int i = 0; i < length1; i++) {
        if (columnVectorsDifferNullForSameIndex(cv1, cv2, i)) {
          return false;
        }
        if (!cv1.isNull[i] && !cv2.isNull[i] && cv1.vector[i] != cv2.vector[i]) {
          return false;
        }
      }
    } else {
      return false;
    }
    return true;
  }

  private boolean compareDecimalColumnVector(DecimalColumnVector cv1, DecimalColumnVector cv2) {
    int length1 = cv1.vector.length;
    int length2 = cv2.vector.length;
    if (length1 == length2 && cv1.scale == cv2.scale && cv1.precision == cv2.precision) {
      for (int i = 0; i < length1; i++) {
        if (columnVectorsDifferNullForSameIndex(cv1, cv2, i)) {
          return false;
        }
        if (!cv1.isNull[i] && !cv2.isNull[i] && !cv1.vector[i].equals(cv2.vector[i])) {
          return false;
        }
      }
    } else {
      return false;
    }
    return true;
  }

  private boolean compareBytesColumnVector(BytesColumnVector cv1, BytesColumnVector cv2) {
    int length1 = cv1.vector.length;
    int length2 = cv2.vector.length;
    if (length1 != length2) {
      return false;
    }

    for (int i = 0; i < length1; i++) {
      // check for different nulls
      if (columnVectorsDifferNullForSameIndex(cv1, cv2, i)) {
        return false;
      }

      // if they are both null, continue
      if (cv1.isNull[i] && cv2.isNull[i]) {
        continue;
      }

      // check if value lengths are the same
      int innerLen1 = cv1.vector[i].length;
      int innerLen2 = cv2.vector[i].length;
      if (innerLen1 != innerLen2) {
        return false;
      }

      // compare value stored
      for (int j = 0; j < innerLen1; j++) {
        if (cv1.vector[i][j] != cv2.vector[i][j]) {
          return false;
        }
      }
    }
    return true;
  }


  private boolean columnVectorsDifferNullForSameIndex(ColumnVector cv1, ColumnVector cv2, int index) {
    return (cv1.isNull[index] && !cv2.isNull[index]) || (!cv1.isNull[index] && cv2.isNull[index]);
  }
}
