package org.apache.hive.benchmark.probe;

import org.apache.hadoop.hive.common.type.RandomTypeUtil;
import org.apache.hadoop.hive.llap.io.probe.OrcProbeHashTable;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastBytesHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastLongHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastLongHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastLongHashSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastMultiKeyHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastMultiKeyHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastMultiKeyHashSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastStringHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastStringHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastStringHashSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMap;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.io.BytesWritable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public abstract class AbstractProbeHashTableBench {

  private static int INIT_CAPACITY = 8;
  private static float LOAD_FACTOR = 0.75f;
  private static int WBUFFER_SIZE = 128;

  protected final VectorizedRowBatch FILTER_CONTEXT = new VectorizedRowBatch(1);
  protected OrcProbeHashTable probeLongHashTable;
  protected ColumnVector filterColumnVector;

//  @Param({"0.1", "0.2", "0.4", "0.8", "1.0"})
  @Param({"0.01", "0.2"})
  double SELECT_PERCENT;

  @Setup
  public abstract void setup() throws HiveException, IOException;

  protected LongColumnVector getLongColumnVector() {
    LongColumnVector columnVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    Random random = new Random();
    for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
      columnVector.vector[i] = random.nextLong();
    }
    return columnVector;
  }

  protected VectorMapJoinHashTable getLongHashMap(LongColumnVector lcv) throws HiveException, IOException {
    VectorMapJoinHashTable ht =
        new VectorMapJoinFastLongHashMap(false, false, VectorMapJoinDesc.HashTableKeyType.LONG,
            INIT_CAPACITY, LOAD_FACTOR, WBUFFER_SIZE, -1);
    Random random = new Random();
    byte[] value = new byte[random.nextInt(1000)];

    int selectedRows = (int) (SELECT_PERCENT * VectorizedRowBatch.DEFAULT_SIZE);
    int row_num = 0;
    while (row_num < selectedRows) {
      int selected_row = random.nextInt(1024);
      VectorMapJoinLongHashMap hashMap = (VectorMapJoinLongHashMap)ht;
      VectorMapJoinHashMapResult hashMapResult = hashMap.createHashMapResult();
      if (hashMap.lookup(lcv.vector[selected_row], hashMapResult) != JoinUtil.JoinResult.MATCH) {
        ((VectorMapJoinFastLongHashMap) ht).testPutRow(lcv.vector[selected_row], value);
        row_num ++;
      }
    }
    if (ht.size() != selectedRows)
      throw new RuntimeException("HT-selectivity size miss-match! Size: " + ht.size() + " Selectivity: " + selectedRows);
    return ht;
  }

  protected VectorMapJoinHashTable getLongHashSet(LongColumnVector lcv) throws HiveException, IOException {
    VectorMapJoinHashTable ht =
        new VectorMapJoinFastLongHashSet(false, false, VectorMapJoinDesc.HashTableKeyType.LONG,
            INIT_CAPACITY, LOAD_FACTOR, WBUFFER_SIZE, -1);
    Random random = new Random();
    byte[] value = new byte[random.nextInt(1000)];

    int selectedRows = (int) (SELECT_PERCENT * VectorizedRowBatch.DEFAULT_SIZE);
    int row_num = 0;
    while (row_num < selectedRows) {
      int selected_row = random.nextInt(1024);
      VectorMapJoinFastLongHashSet hashMap = (VectorMapJoinFastLongHashSet)ht;
      VectorMapJoinHashSetResult hashMapResult = hashMap.createHashSetResult();
      if (hashMap.contains(lcv.vector[selected_row], hashMapResult) != JoinUtil.JoinResult.MATCH) {
        ((VectorMapJoinFastLongHashSet) ht).testPutRow(lcv.vector[selected_row]);
        row_num ++;
      }
    }
    if (ht.size() != selectedRows)
      throw new RuntimeException("HT-selectivity size miss-match! Size: " + ht.size() + " Selectivity: " + selectedRows);
    return ht;
  }

  protected VectorMapJoinHashTable getLongHashMultiSet(LongColumnVector lcv) throws HiveException, IOException {
    VectorMapJoinHashTable ht =
        new VectorMapJoinFastLongHashMultiSet(false, false, VectorMapJoinDesc.HashTableKeyType.LONG,
            INIT_CAPACITY, LOAD_FACTOR, WBUFFER_SIZE, -1);
    Random random = new Random();
    byte[] value = new byte[random.nextInt(1000)];

    int selectedRows = (int) (SELECT_PERCENT * VectorizedRowBatch.DEFAULT_SIZE);
    int row_num = 0;
    while (row_num < selectedRows) {
      int selected_row = random.nextInt(1024);
      VectorMapJoinFastLongHashMultiSet hashMap = (VectorMapJoinFastLongHashMultiSet)ht;
      VectorMapJoinHashMultiSetResult hashMapResult = hashMap.createHashMultiSetResult();
      if (hashMap.contains(lcv.vector[selected_row], hashMapResult) != JoinUtil.JoinResult.MATCH) {
        ((VectorMapJoinFastLongHashMultiSet) ht).testPutRow(lcv.vector[selected_row]);
        row_num ++;
      }
    }
    if (ht.size() != selectedRows)
      throw new RuntimeException("HT-selectivity size miss-match! Size: " + ht.size() + " Selectivity: " + selectedRows);
    return ht;
  }

  protected BytesColumnVector getBytesColumnVector() {
    BytesColumnVector columnVector = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    Random random = new Random();
    int str_length = 8;
    for (int row = 0; row != VectorizedRowBatch.DEFAULT_SIZE; row++) {
      columnVector.vector[row] = new byte[str_length];
      columnVector.start[row] = 0;
      columnVector.length[row] = str_length;
      for (int j = 0; j < str_length; j++) {
        columnVector.vector[row][j] = (byte) (random.nextInt(+'c' - 'a' + 1) + 'a');
      }
    }
    return columnVector;
  }

  protected VectorMapJoinHashTable getBytesHashMap(BytesColumnVector bcv) throws HiveException, IOException {
    VectorMapJoinHashTable ht =
        new VectorMapJoinFastStringHashMap(false, INIT_CAPACITY, LOAD_FACTOR, WBUFFER_SIZE, -1);
    Random random = new Random();
    byte[] strRandValue = new byte[10];
    for (int i = 0; i < 10; i++) {
      strRandValue[i] = (byte) (random.nextInt(+'c' - 'a' + 1) + 'a');
    }
    // Key BinarySerializer
    BinarySortableSerializeWrite keySerializeWrite = new BinarySortableSerializeWrite(1);
    Output currKeyOut = new Output();

    int selectedRows = (int) (SELECT_PERCENT * VectorizedRowBatch.DEFAULT_SIZE);
    int row_num = 0;
    while (row_num < selectedRows) {
      keySerializeWrite.set(currKeyOut);
      keySerializeWrite.reset();

      int selected_row = random.nextInt(1024);
      keySerializeWrite.writeString(bcv.vector[selected_row], bcv.start[selected_row] , bcv.length[selected_row]);

      VectorMapJoinHashMapResult result = ((VectorMapJoinFastBytesHashMap)ht).createHashMapResult();
      if (((VectorMapJoinFastBytesHashMap)ht).lookup(bcv.vector[selected_row], bcv.start[selected_row], bcv.length[selected_row],
          result) != JoinUtil.JoinResult.MATCH) {
        ((VectorMapJoinFastStringHashMap) ht).
            putRow(new BytesWritable(Arrays.copyOf(currKeyOut.getData(), currKeyOut.getLength())), new BytesWritable(Arrays.copyOf(strRandValue, 10)));
        row_num ++;
      }
    }
    if (ht.size() != selectedRows)
      throw new RuntimeException("HT-selectivity size miss-match! Size: " + ht.size() + " Selectivity: " + selectedRows);
    return ht;
  }

  protected VectorMapJoinHashTable getBytesHashSet(BytesColumnVector bcv) throws HiveException, IOException {
    VectorMapJoinHashTable ht =
        new VectorMapJoinFastStringHashSet(false, INIT_CAPACITY, LOAD_FACTOR, WBUFFER_SIZE, -1);
    Random random = new Random();
    byte[] strRandValue = new byte[10];
    for (int i = 0; i < 10; i++) {
      strRandValue[i] = (byte) (random.nextInt(+'c' - 'a' + 1) + 'a');
    }
    // Key BinarySerializer
    BinarySortableSerializeWrite keySerializeWrite = new BinarySortableSerializeWrite(1);
    Output currKeyOut = new Output();

    int selectedRows = (int) (SELECT_PERCENT * VectorizedRowBatch.DEFAULT_SIZE);
    int row_num = 0;
    while (row_num < selectedRows) {
      keySerializeWrite.set(currKeyOut);
      keySerializeWrite.reset();

      int selected_row = random.nextInt(1024);
      keySerializeWrite.writeString(bcv.vector[selected_row], bcv.start[selected_row] , bcv.length[selected_row]);

      VectorMapJoinHashSetResult result = ((VectorMapJoinFastStringHashSet)ht).createHashSetResult();
      if (((VectorMapJoinFastStringHashSet)ht).contains(bcv.vector[selected_row], bcv.start[selected_row], bcv.length[selected_row],
          result) != JoinUtil.JoinResult.MATCH) {
        ((VectorMapJoinFastStringHashSet) ht).
            putRow(new BytesWritable(Arrays.copyOf(currKeyOut.getData(), currKeyOut.getLength())), new BytesWritable(Arrays.copyOf(strRandValue, 10)));
        row_num ++;
      }
    }
    if (ht.size() != selectedRows)
      throw new RuntimeException("HT-selectivity size miss-match! Size: " + ht.size() + " Selectivity: " + selectedRows);
    return ht;
  }

  protected VectorMapJoinHashTable getBytesHashMultiSet(BytesColumnVector bcv) throws HiveException, IOException {
    VectorMapJoinHashTable ht =
        new VectorMapJoinFastStringHashMultiSet(false, INIT_CAPACITY, LOAD_FACTOR, WBUFFER_SIZE, -1);
    Random random = new Random();
    byte[] strRandValue = new byte[10];
    for (int i = 0; i < 10; i++) {
      strRandValue[i] = (byte) (random.nextInt(+'c' - 'a' + 1) + 'a');
    }
    // Key BinarySerializer
    BinarySortableSerializeWrite keySerializeWrite = new BinarySortableSerializeWrite(1);
    Output currKeyOut = new Output();

    int selectedRows = (int) (SELECT_PERCENT * VectorizedRowBatch.DEFAULT_SIZE);
    int row_num = 0;
    while (row_num < selectedRows) {
      keySerializeWrite.set(currKeyOut);
      keySerializeWrite.reset();

      int selected_row = random.nextInt(1024);
      keySerializeWrite.writeString(bcv.vector[selected_row], bcv.start[selected_row] , bcv.length[selected_row]);

      VectorMapJoinHashMultiSetResult result = ((VectorMapJoinFastStringHashMultiSet)ht).createHashMultiSetResult();
      if (((VectorMapJoinFastStringHashMultiSet)ht).contains(bcv.vector[selected_row], bcv.start[selected_row], bcv.length[selected_row],
          result) != JoinUtil.JoinResult.MATCH) {
        ((VectorMapJoinFastStringHashMultiSet) ht).
            putRow(new BytesWritable(Arrays.copyOf(currKeyOut.getData(), currKeyOut.getLength())), new BytesWritable(Arrays.copyOf(strRandValue, 10)));
        row_num ++;
      }
    }
    if (ht.size() != selectedRows)
      throw new RuntimeException("HT-selectivity size miss-match! Size: " + ht.size() + " Selectivity: " + selectedRows);
    return ht;
  }

  protected TimestampColumnVector getTimestampColumnVector() {
    TimestampColumnVector columnVector = new TimestampColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    Random random = new Random();
    for (int row = 0; row != VectorizedRowBatch.DEFAULT_SIZE; row++) {
      columnVector.set(row, RandomTypeUtil.getRandTimestamp(random));
    }
    return columnVector;
  }

  protected VectorMapJoinHashTable getTimestampHashMap(TimestampColumnVector bcv) throws HiveException, IOException {
    VectorMapJoinHashTable ht =
        new VectorMapJoinFastMultiKeyHashMap(false, INIT_CAPACITY, LOAD_FACTOR, WBUFFER_SIZE, -1);;
    // Key BinarySerializer
    BinarySortableSerializeWrite keySerializeWrite = new BinarySortableSerializeWrite(1);
    Output currKeyOut = new Output();
    Random random = new Random();

    int selectedRows = (int) (SELECT_PERCENT * VectorizedRowBatch.DEFAULT_SIZE);
    int row_num = 0;
    while (row_num < selectedRows) {
      keySerializeWrite.set(currKeyOut);
      keySerializeWrite.reset();

      int selected_row = random.nextInt(1024);
      java.sql.Timestamp currTS = bcv.asScratchTimestamp(selected_row);
      keySerializeWrite.writeTimestamp(org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(currTS.getTime(),
          currTS.getNanos()));


      VectorMapJoinHashMapResult result  =  ((VectorMapJoinFastBytesHashMap) ht).createHashMapResult();
      if (((VectorMapJoinFastBytesHashMap)ht).lookup(currKeyOut.getData(), 0, currKeyOut.getLength(),
          result) != JoinUtil.JoinResult.MATCH) {
        ((VectorMapJoinFastBytesHashMap) ht)
            .putRow(new BytesWritable(Arrays.copyOf(currKeyOut.getData(), currKeyOut.getLength())),
                new BytesWritable(Arrays.copyOf(currKeyOut.getData(), currKeyOut.getLength())));
        row_num++;
      }
    }
    if (ht.size() != selectedRows)
      throw new RuntimeException("HT-selectivity size miss-match! Size: " + ht.size() + " Selectivity: " + selectedRows);
    return ht;
  }

  protected VectorMapJoinHashTable getTimestampHashSet(TimestampColumnVector bcv) throws HiveException, IOException {
    VectorMapJoinHashTable ht =
        new VectorMapJoinFastMultiKeyHashSet(false, INIT_CAPACITY, LOAD_FACTOR, WBUFFER_SIZE, -1);;
    // Key BinarySerializer
    BinarySortableSerializeWrite keySerializeWrite = new BinarySortableSerializeWrite(1);
    Output currKeyOut = new Output();
    Random random = new Random();

    int selectedRows = (int) (SELECT_PERCENT * VectorizedRowBatch.DEFAULT_SIZE);
    int row_num = 0;
    while (row_num < selectedRows) {
      keySerializeWrite.set(currKeyOut);
      keySerializeWrite.reset();

      int selected_row = random.nextInt(1024);
      java.sql.Timestamp currTS = bcv.asScratchTimestamp(selected_row);
      keySerializeWrite.writeTimestamp(org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(currTS.getTime(),
          currTS.getNanos()));


      VectorMapJoinHashSetResult result  =  ((VectorMapJoinFastMultiKeyHashSet) ht).createHashSetResult();
      if (((VectorMapJoinFastMultiKeyHashSet)ht).contains(currKeyOut.getData(), 0, currKeyOut.getLength(),
          result) != JoinUtil.JoinResult.MATCH) {
        ((VectorMapJoinFastMultiKeyHashSet) ht)
            .putRow(new BytesWritable(Arrays.copyOf(currKeyOut.getData(), currKeyOut.getLength())),
                new BytesWritable(Arrays.copyOf(currKeyOut.getData(), currKeyOut.getLength())));
        row_num++;
      }
    }
    if (ht.size() != selectedRows)
      throw new RuntimeException("HT-selectivity size miss-match! Size: " + ht.size() + " Selectivity: " + selectedRows);
    return ht;
  }

  protected VectorMapJoinHashTable getTimestampHashMultiSet(TimestampColumnVector bcv) throws HiveException, IOException {
    VectorMapJoinHashTable ht =
        new VectorMapJoinFastMultiKeyHashMultiSet(false, INIT_CAPACITY, LOAD_FACTOR, WBUFFER_SIZE, -1);;
    // Key BinarySerializer
    BinarySortableSerializeWrite keySerializeWrite = new BinarySortableSerializeWrite(1);
    Output currKeyOut = new Output();
    Random random = new Random();

    int selectedRows = (int) (SELECT_PERCENT * VectorizedRowBatch.DEFAULT_SIZE);
    int row_num = 0;
    while (row_num < selectedRows) {
      keySerializeWrite.set(currKeyOut);
      keySerializeWrite.reset();

      int selected_row = random.nextInt(1024);
      java.sql.Timestamp currTS = bcv.asScratchTimestamp(selected_row);
      keySerializeWrite.writeTimestamp(org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(currTS.getTime(),
          currTS.getNanos()));


      VectorMapJoinHashMultiSetResult result  =  ((VectorMapJoinFastMultiKeyHashMultiSet) ht).createHashMultiSetResult();
      if (((VectorMapJoinFastMultiKeyHashMultiSet)ht).contains(currKeyOut.getData(), 0, currKeyOut.getLength(),
          result) != JoinUtil.JoinResult.MATCH) {
        ((VectorMapJoinFastMultiKeyHashMultiSet) ht)
            .putRow(new BytesWritable(Arrays.copyOf(currKeyOut.getData(), currKeyOut.getLength())),
                new BytesWritable(Arrays.copyOf(currKeyOut.getData(), currKeyOut.getLength())));
        row_num++;
      }
    }
    if (ht.size() != selectedRows)
      throw new RuntimeException("HT-selectivity size miss-match! Size: " + ht.size() + " Selectivity: " + selectedRows);
    return ht;
  }

  @Benchmark
  @Measurement(batchSize = 1_000, iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
  public void bench() {
    this.probeLongHashTable.filterColumnVector(filterColumnVector, FILTER_CONTEXT, VectorizedRowBatch.DEFAULT_SIZE);
  }
}
