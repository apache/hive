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

package org.apache.hadoop.hive.ql.exec.vector.util;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.TestVectorizedORCReader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

/**
 * This class generates an orc file from a specified record class. The orc file
 * will contain 3 batches worth of rows for each column for all kinds of data distribution:
 * all values, no nulls, repeating value, and repeating null.
 *
 */
public class OrcFileGenerator {

  enum BatchDataDistribution {
    AllValues,
    NoNulls,
    RepeatingValue,
    RepeatingNull
  }

  /**
   * Base class for type specific batch generators. Each type specific batch generator implements
   * generateRandomNonNullValue to generate random values, and initializeFixedPointValues to
   * specify a set of fixed values within the data (this is useful when defining query predicates)
   */
  private abstract static class BatchGenerator<T> {

    private final Random rand = new Random(0xfa57);
    private int possibleNonRandomValueGenerated = rand.nextInt();
    private final T[] fixedPointValues;

    public BatchGenerator() {
      fixedPointValues = initializeFixedPointValues();
    }

    protected abstract T[] initializeFixedPointValues();

    protected abstract T generateRandomNonNullValue(Random rand);

    public T[] generateBatch(BatchDataDistribution dist) {

      Object[] batch = new Object[VectorizedRowBatch.DEFAULT_SIZE];

      for (int i = 0; i < batch.length; i++) {
        switch (dist) {
        case AllValues:
          if (possibleNonRandomValueGenerated % 73 == 0) {
            batch[i] = null;
          } else if (fixedPointValues != null && possibleNonRandomValueGenerated % 233 == 0) {
            batch[i] = fixedPointValues[rand.nextInt(fixedPointValues.length)];
          } else {
            batch[i] = generateRandomNonNullValue(rand);
          }
          possibleNonRandomValueGenerated++;
          break;

        case NoNulls:
          if (fixedPointValues != null && possibleNonRandomValueGenerated % 233 == 0) {
            batch[i] = fixedPointValues[rand.nextInt(fixedPointValues.length)];
          } else {
            batch[i] = generateRandomNonNullValue(rand);
          }
          possibleNonRandomValueGenerated++;
          break;

        case RepeatingNull:
          batch[i] = null;
          break;

        case RepeatingValue:
          if (i == 0) {
            batch[i] = generateRandomNonNullValue(rand);
          } else {
            batch[i] = batch[0];
          }
          break;

        default:
          throw new UnsupportedOperationException(
              dist.toString() + " data distribution is not implemented.");
        }
      }

      return (T[]) batch;
    }
  }

  private static class ByteBatchGenerator extends BatchGenerator<Byte> {

    @Override
    protected Byte generateRandomNonNullValue(Random rand) {
      return (byte) (rand.nextInt((Byte.MAX_VALUE - Byte.MIN_VALUE) / 2)
          - Math.abs(Byte.MIN_VALUE / 2));
    }

    @Override
    protected Byte[] initializeFixedPointValues() {
      return new Byte[] {-23, -1, 17, 33};
    }
  }

  private static class ShortBatchGenerator extends BatchGenerator<Short> {

    @Override
    protected Short generateRandomNonNullValue(Random rand) {
      return (short) (rand.nextInt((Short.MAX_VALUE - Short.MIN_VALUE) / 2)
          + (Short.MIN_VALUE / 2));
    }

    @Override
    protected Short[] initializeFixedPointValues() {
      return new Short[] {-257, -75, 197, 359};
    }

  }

  private static class IntegerBatchGenerator extends BatchGenerator<Integer> {
    @Override
    protected Integer generateRandomNonNullValue(Random rand) {
      return rand.nextInt(Integer.MAX_VALUE) + (Integer.MIN_VALUE / 2);
    }

    @Override
    protected Integer[] initializeFixedPointValues() {
      return new Integer[] {-3728, -563, 762, 6981};
    }
  }

  private static class LongBatchGenerator extends BatchGenerator<Long> {

    @Override
    protected Long generateRandomNonNullValue(Random rand) {
      return (long) rand.nextInt();
    }

    @Override
    protected Long[] initializeFixedPointValues() {
      return new Long[] {(long) -89010, (long) -6432, (long) 3569, (long) 988888};
    }
  }

  private static class FloatBatchGenerator extends BatchGenerator<Float> {

    private final ByteBatchGenerator byteGenerator = new ByteBatchGenerator();

    @Override
    protected Float generateRandomNonNullValue(Random rand) {
      return (float) byteGenerator.generateRandomNonNullValue(rand);
    }

    @Override
    protected Float[] initializeFixedPointValues() {
      return new Float[] {(float) -26.28, (float) -1.389, (float) 10.175, (float) 79.553};
    }

  }

  private static class DoubleBatchGenerator extends BatchGenerator<Double> {

    private final ShortBatchGenerator shortGenerator = new ShortBatchGenerator();

    @Override
    protected Double generateRandomNonNullValue(Random rand) {
      return (double) shortGenerator.generateRandomNonNullValue(rand);
    }

    @Override
    protected Double[] initializeFixedPointValues() {
      return new Double[] {-5638.15, -863.257, 2563.58, 9763215.5639};
    }

  }

  private static class BooleanBatchGenerator extends BatchGenerator<Boolean> {
    @Override
    protected Boolean generateRandomNonNullValue(Random rand) {
      return rand.nextBoolean();
    }

    @Override
    protected Boolean[] initializeFixedPointValues() {
      return null;
    }
  }

  private static class StringBatchGenerator extends BatchGenerator<String> {

    @Override
    protected String generateRandomNonNullValue(Random rand) {
      int length = rand.nextInt(20) + 5;
      char[] values = new char[length];
      for (int j = 0; j < length; j++) {
        switch (rand.nextInt(3)) {
        case 0:
          values[j] = (char) (rand.nextInt((int) 'z' - (int) 'a') + (int) 'a');
          break;
        case 1:
          values[j] = (char) (rand.nextInt((int) 'Z' - (int) 'A') + (int) 'A');
          break;
        case 2:
          values[j] = (char) (rand.nextInt((int) '9' - (int) '0') + (int) '0');
          break;
        default:
          throw new UnsupportedOperationException();
        }
      }
      return new String(values);
    }

    @Override
    protected String[] initializeFixedPointValues() {
      return new String[] {"a", "b", "ss", "10"};
    }

  }

  private static class TimestampBatchGenerator extends BatchGenerator<Timestamp> {

    private final ShortBatchGenerator shortGen = new ShortBatchGenerator();

    @Override
    protected Timestamp generateRandomNonNullValue(Random rand) {
      return new Timestamp(shortGen.generateRandomNonNullValue(rand));
    }

    @Override
    protected Timestamp[] initializeFixedPointValues() {
      // TODO Auto-generated method stub
      return new Timestamp[] {
          new Timestamp(-29071),
          new Timestamp(-10669),
          new Timestamp(16558),
          new Timestamp(31808)
      };
    }
  }

  private static final Map<Class, BatchGenerator> TYPE_TO_BATCH_GEN_MAP;
  static {
    TYPE_TO_BATCH_GEN_MAP = new HashMap<Class, BatchGenerator>();
    TYPE_TO_BATCH_GEN_MAP.put(Boolean.class, new BooleanBatchGenerator());

    TYPE_TO_BATCH_GEN_MAP.put(Byte.class, new ByteBatchGenerator());
    TYPE_TO_BATCH_GEN_MAP.put(Integer.class, new IntegerBatchGenerator());
    TYPE_TO_BATCH_GEN_MAP.put(Long.class, new LongBatchGenerator());
    TYPE_TO_BATCH_GEN_MAP.put(Short.class, new ShortBatchGenerator());

    TYPE_TO_BATCH_GEN_MAP.put(Float.class, new FloatBatchGenerator());
    TYPE_TO_BATCH_GEN_MAP.put(Double.class, new DoubleBatchGenerator());

    TYPE_TO_BATCH_GEN_MAP.put(String.class, new StringBatchGenerator());

    TYPE_TO_BATCH_GEN_MAP.put(Timestamp.class, new TimestampBatchGenerator());
  }

  /**
   * Generates an orc file based on the provided record class in the specified file system
   * at the output path.
   *
   * @param conf the configuration used to initialize the orc writer
   * @param fs the file system to which will contain the generated orc file
   * @param outputPath the path where the generated orc will be placed
   * @param recordClass a class the defines the record format for the generated orc file, this
   * class must have exactly one constructor.
   */
  public static void generateOrcFile(Configuration conf, FileSystem fs, Path outputPath,
      Class recordClass)
      throws IOException, InstantiationException,
      IllegalAccessException, InvocationTargetException {

    ObjectInspector inspector;
    synchronized (TestVectorizedORCReader.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          recordClass, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    Writer writer = OrcFile.createWriter(
        fs,
        outputPath,
        conf,
        inspector,
        100000,
        CompressionKind.ZLIB,
        10000,
        10000);

    try {
      Constructor[] constructors = recordClass.getConstructors();

      if (constructors.length != 1) {
        throw new UnsupportedOperationException(
            "The provided recordClass must have exactly one constructor.");
      }

      BatchDataDistribution[] dataDist = BatchDataDistribution.values();
      Class[] columns = constructors[0].getParameterTypes();
      for (int i = 0; i < dataDist.length * 3; i++) {
        Object[][] rows = new Object[columns.length][VectorizedRowBatch.DEFAULT_SIZE];
        for (int c = 0; c < columns.length; c++) {
          if (!TYPE_TO_BATCH_GEN_MAP.containsKey(columns[c])) {
            throw new UnsupportedOperationException("No batch generator defined for type "
                + columns[c].getName());
          }
          rows[c] = TYPE_TO_BATCH_GEN_MAP.get(
              columns[c]).generateBatch(dataDist[(i + c) % dataDist.length]);
        }

        for (int r = 0; r < VectorizedRowBatch.DEFAULT_SIZE; r++) {
          Object[] row = new Object[columns.length];
          for (int c = 0; c < columns.length; c++) {
            row[c] = rows[c][r];
          }
          writer.addRow(
              constructors[0].newInstance(row));
        }
      }
    } finally {
      writer.close();
    }
  }
}
