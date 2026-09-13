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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.llap.io.api.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.ReadPipeline;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.impl.SchemaEvolution;
import org.junit.Test;

/**
 * {@link LlapRecordReader} over a scripted read pipeline: when its IO starts, and how a decoded
 * batch reaches the row batch.
 */
public class TestLlapRecordReader {

  private static final int ROWS = 5;

  private final InlineExecutor executor = new InlineExecutor();
  private ColumnVectorBatch cvb;

  /** A two-column plan. */
  private static JobConf job() {
    JobConf job = new JobConf(new HiveConf());
    job.set(IOConstants.COLUMNS, "id,val");
    job.set(IOConstants.COLUMNS_TYPES, "bigint,bigint");
    ColumnProjectionUtils.setReadColumns(job, List.of(0, 1));
    HiveConf.setVar(job, HiveConf.ConfVars.PLAN, "//tmp");
    MapWork mapWork = new MapWork();
    TypeInfo[] types = {TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo};
    mapWork.setVectorizedRowBatchCtx(new VectorizedRowBatchCtx(new String[] {"id", "val"}, types, null,
        new int[] {0, 1}, 0, 0, new VirtualColumn[0], new String[0], null));
    Utilities.setMapWork(job, mapWork);
    return job;
  }

  /** {@code id} is the physical row, {@code val} ten times that. */
  private static ColumnVectorBatch batch() {
    ColumnVectorBatch cvb = new ColumnVectorBatch(2);
    for (int c = 0; c < 2; c++) {
      LongColumnVector column = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      for (int i = 0; i < ROWS; i++) {
        column.vector[i] = c == 0 ? i : 10L * i;
      }
      cvb.cols[c] = column;
    }
    cvb.size = ROWS;
    return cvb;
  }

  private LlapRecordReader reader(JobConf job) throws Exception {
    LlapRecordReader reader = LlapRecordReader.create(job, new FileSplit(new Path("/data"), 0, 1, (String[]) null),
        List.of(0, 1), "host", new ScriptedProducer(), executor, null, null, Reporter.NULL, new HiveConf());
    assertNotNull(reader);
    return reader;
  }

  @Test
  public void aDecodedBatchReachesTheRowBatch() throws Exception {
    cvb = batch();
    LlapRecordReader reader = reader(job());
    reader.start();
    VectorizedRowBatch vrb = reader.createValue();
    assertTrue(reader.next(NullWritable.get(), vrb));
    assertFalse(vrb.selectedInUse);
    assertEquals(ROWS, vrb.size);
    for (int i = 0; i < ROWS; i++) {
      assertEquals(i, ((LongColumnVector) vrb.cols[0]).vector[i]);
      assertEquals(10L * i, ((LongColumnVector) vrb.cols[1]).vector[i]);
    }
    assertFalse(reader.next(NullWritable.get(), vrb));
    reader.close();
  }

  @Test
  public void readerStartsItsIoOnStart() throws Exception {
    cvb = batch();
    LlapRecordReader reader = reader(job());
    assertEquals(0, executor.runs);
    reader.start();
    assertEquals(1, executor.runs);
    reader.close();
  }

  /** Hands {@link #cvb} to the reader and finishes, all on the caller's thread. */
  private final class ScriptedProducer implements ColumnVectorProducer {

    @Override
    public ReadPipeline createReadPipeline(Consumer<ColumnVectorBatch> consumer, FileSplit split,
        ColumnVectorProducer.Includes includes, SearchArgument sarg, QueryFragmentCounters counters,
        ColumnVectorProducer.SchemaEvolutionFactory sef, InputFormat<?, ?> sourceInputFormat,
        Deserializer sourceSerDe, Reporter reporter, JobConf job, Map<Path, PartitionDesc> parts) {
      return new ReadPipeline() {
        @Override
        public Callable<Void> getReadCallable() {
          return () -> {
            consumer.consumeData(cvb);
            consumer.setDone();
            return null;
          };
        }

        @Override
        public SchemaEvolution getSchemaEvolution() {
          return null;
        }

        @Override
        public void pause() {
        }

        @Override
        public void unpause() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void returnData(ColumnVectorBatch data) {
        }
      };
    }
  }

  /** Runs every submitted task on the submitting thread and counts them. */
  private static final class InlineExecutor extends AbstractExecutorService {
    int runs;

    @Override
    public void execute(Runnable command) {
      runs++;
      command.run();
    }

    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }
  }
}
