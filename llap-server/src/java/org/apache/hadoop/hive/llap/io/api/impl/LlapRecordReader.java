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

package org.apache.hadoop.hive.llap.io.api.impl;

import java.util.ArrayList;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.counters.FragmentCountersMap;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.daemon.impl.StatsRecordingThreadPool;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer.Includes;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer.SchemaEvolutionFactory;
import org.apache.hadoop.hive.llap.io.decode.ReadPipeline;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcRecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.VectorizedOrcAcidRowBatchReader;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.tez.common.counters.TezCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

class LlapRecordReader
    implements RecordReader<NullWritable, VectorizedRowBatch>, Consumer<ColumnVectorBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(LlapRecordReader.class);
  private static final Object DONE_OBJECT = new Object();

  private final FileSplit split;
  private final IncludesImpl includes;
  private final SearchArgument sarg;
  private final VectorizedRowBatchCtx rbCtx;
  private final Object[] partitionValues;

  private final LinkedBlockingQueue<Object> queue;
  private final AtomicReference<Throwable> pendingError = new AtomicReference<>(null);

  /** Vector that is currently being processed by our user. */
  private ColumnVectorBatch lastCvb = null;
  private boolean isFirst = true;
  private int maxQueueSize = 0;

  private boolean isClosed = false, isInterrupted = false;
  private final ConsumerFeedback<ColumnVectorBatch> feedback;
  private final QueryFragmentCounters counters;
  private long firstReturnTime;

  private final JobConf jobConf;
  private final ReadPipeline rp;
  private final ExecutorService executor;
  private final boolean isAcidScan;

  /**
   * Creates the record reader and checks the input-specific compatibility.
   * @return The reader if the split can be read, null otherwise.
   */
  public static LlapRecordReader create(JobConf job, FileSplit split,
      List<Integer> tableIncludedCols, String hostName, ColumnVectorProducer cvp,
      ExecutorService executor, InputFormat<?, ?> sourceInputFormat, Deserializer sourceSerDe,
      Reporter reporter, Configuration daemonConf) throws IOException, HiveException {
    MapWork mapWork = findMapWork(job);
    if (mapWork == null) return null; // No compatible MapWork.
    LlapRecordReader rr = new LlapRecordReader(mapWork, job, split, tableIncludedCols, hostName,
        cvp, executor, sourceInputFormat, sourceSerDe, reporter, daemonConf);
    if (!rr.checkOrcSchemaEvolution()) {
      rr.close();
      return null;
    }
    return rr;
  }

  private LlapRecordReader(MapWork mapWork, JobConf job, FileSplit split,
      List<Integer> tableIncludedCols, String hostName, ColumnVectorProducer cvp,
      ExecutorService executor, InputFormat<?, ?> sourceInputFormat, Deserializer sourceSerDe,
      Reporter reporter, Configuration daemonConf) throws IOException, HiveException {
    this.executor = executor;
    this.jobConf = job;
    this.split = split;

    this.sarg = ConvertAstToSearchArg.createFromConf(job);
    final String fragmentId = LlapTezUtils.getFragmentId(job);
    final String dagId = LlapTezUtils.getDagId(job);
    final String queryId = HiveConf.getVar(job, HiveConf.ConfVars.HIVEQUERYID);
    MDC.put("dagId", dagId);
    MDC.put("queryId", queryId);
    TezCounters taskCounters = null;
    if (fragmentId != null) {
      MDC.put("fragmentId", fragmentId);
      taskCounters = FragmentCountersMap.getCountersForFragment(fragmentId);
      LOG.info("Received fragment id: {}", fragmentId);
    } else {
      LOG.warn("Not using tez counters as fragment id string is null");
    }
    this.counters = new QueryFragmentCounters(job, taskCounters);
    this.counters.setDesc(QueryFragmentCounters.Desc.MACHINE, hostName);

    VectorizedRowBatchCtx ctx = mapWork.getVectorizedRowBatchCtx();
    rbCtx = ctx != null ? ctx : LlapInputFormat.createFakeVrbCtx(mapWork);

    isAcidScan = AcidUtils.isFullAcidScan(jobConf);
    TypeDescription schema = OrcInputFormat.getDesiredRowTypeDescr(
        job, isAcidScan, Integer.MAX_VALUE);

    this.includes = new IncludesImpl(tableIncludedCols, isAcidScan, rbCtx, schema, job);

    int queueLimitBase = getQueueVar(ConfVars.LLAP_IO_VRB_QUEUE_LIMIT_BASE, job, daemonConf);
    int queueLimitMin =  getQueueVar(ConfVars.LLAP_IO_VRB_QUEUE_LIMIT_MIN, job, daemonConf);
    int limit = determineQueueLimit(queueLimitBase, queueLimitMin, rbCtx.getRowColumnTypeInfos());
    LOG.info("Queue limit for LlapRecordReader is " + limit);
    this.queue = new LinkedBlockingQueue<>(limit);


    int partitionColumnCount = rbCtx.getPartitionColumnCount();
    if (partitionColumnCount > 0) {
      partitionValues = new Object[partitionColumnCount];
      VectorizedRowBatchCtx.getPartitionValues(rbCtx, mapWork, split, partitionValues);
    } else {
      partitionValues = null;
    }

    // Create the consumer of encoded data; it will coordinate decoding to CVBs.
    feedback = rp = cvp.createReadPipeline(this, split, includes, sarg, counters, includes,
        sourceInputFormat, sourceSerDe, reporter, job, mapWork.getPathToPartitionInfo());
  }

  private static int getQueueVar(ConfVars var, JobConf jobConf, Configuration daemonConf) {
    // Check job config for overrides, otherwise use the default server value.
    int jobVal = jobConf.getInt(var.varname, -1);
    return (jobVal != -1) ? jobVal : HiveConf.getIntVar(daemonConf, var);
  }

  // For queue size estimation purposes, we assume all columns have weight one, and the following
  // types are counted as multiple columns. This is very primitive; if we wanted to make it better,
  // we'd increase the base limit, and adjust dynamically based on IO and processing perf delays.
  private static final int COL_WEIGHT_COMPLEX = 16, COL_WEIGHT_HIVEDECIMAL = 4,
      COL_WEIGHT_STRING = 8;
  private static int determineQueueLimit(
      int queueLimitBase, int queueLimitMin, TypeInfo[] typeInfos) {
    // If the values are equal, the queue limit is fixed.
    if (queueLimitBase == queueLimitMin) return queueLimitBase;
    // If there are no columns (projection only join?) just assume no weight.
    if (typeInfos == null || typeInfos.length == 0) return queueLimitBase;
    double totalWeight = 0;
    for (TypeInfo ti : typeInfos) {
      int colWeight = 1;
      if (ti.getCategory() != Category.PRIMITIVE) {
        colWeight = COL_WEIGHT_COMPLEX;
      } else {
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo)ti;
        switch (pti.getPrimitiveCategory()) {
        case BINARY:
        case CHAR:
        case VARCHAR:
        case STRING:
          colWeight = COL_WEIGHT_STRING;
        case DECIMAL:
          colWeight = COL_WEIGHT_HIVEDECIMAL;
        default:
          colWeight = 1;
        }
      }
      totalWeight += colWeight;
    }
    return Math.max(queueLimitMin, (int)(queueLimitBase / totalWeight));
  }


  private static MapWork findMapWork(JobConf job) throws HiveException {
    String inputName = job.get(Utilities.INPUT_NAME, null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing for input " + inputName);
    }
    String prefixes = job.get(DagUtils.TEZ_MERGE_WORK_FILE_PREFIXES);
    if (prefixes != null && !StringUtils.isBlank(prefixes)) {
      // Currently SMB is broken, so we cannot check if it's  compatible with IO elevator.
      // So, we don't use the below code that would get the correct MapWork. See HIVE-16985.
      return null;
    }

    BaseWork work = null;
    // HIVE-16985: try to find the fake merge work for SMB join, that is really another MapWork.
    if (inputName != null) {
      if (prefixes == null ||
          !Lists.newArrayList(prefixes.split(",")).contains(inputName)) {
        inputName = null;
      }
    }
    if (inputName != null) {
      work = Utilities.getMergeWork(job, inputName);
    }

    if (work == null || !(work instanceof MapWork)) {
      work = Utilities.getMapWork(job);
    }
    return (MapWork) work;
  }

  /**
   * Starts the data read pipeline
   */
  public void start() {
    // perform the data read asynchronously
    if (executor instanceof StatsRecordingThreadPool) {
      // Every thread created by this thread pool will use the same handler
      ((StatsRecordingThreadPool) executor).setUncaughtExceptionHandler(
          new IOUncaughtExceptionHandler());
    }
    executor.submit(rp.getReadCallable());
  }

  private boolean checkOrcSchemaEvolution() {
    SchemaEvolution evolution = rp.getSchemaEvolution();
    // TODO: should this just use physical IDs?
    for (int i = 0; i < includes.getReaderLogicalColumnIds().size(); ++i) {
      int projectedColId = includes.getReaderLogicalColumnIds().get(i);
      // Adjust file column index for ORC struct.
        int fileColId =  OrcInputFormat.getRootColumn(!isAcidScan) + projectedColId + 1;
      if (!evolution.isPPDSafeConversion(fileColId)) {
        LlapIoImpl.LOG.warn("Unsupported schema evolution! Disabling Llap IO for {}", split);
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean next(NullWritable key, VectorizedRowBatch vrb) throws IOException {
    assert vrb != null;
    if (isClosed) {
      throw new AssertionError("next called after close");
    }
    // Add partition cols if necessary (see VectorizedOrcInputFormat for details).
    boolean wasFirst = isFirst;
    if (isFirst) {
      if (partitionValues != null) {
        rbCtx.addPartitionColsToBatch(vrb, partitionValues);
      }
      isFirst = false;
    }
    ColumnVectorBatch cvb = null;
    try {
      cvb = nextCvb();
    } catch (InterruptedException e) {
      // Query might have been canceled. Stop the background processing.
      feedback.stop();
      isInterrupted = true; // In case we are stuck in consume.
      throw new IOException(e);
    }
    if (cvb == null) {
      if (wasFirst) {
        firstReturnTime = counters.startTimeCounter();
      }
      counters.incrTimeCounter(LlapIOCounters.CONSUMER_TIME_NS, firstReturnTime);
      return false;
    }
    final boolean isVectorized = HiveConf.getBoolVar(jobConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED);
    if (isAcidScan) {
      vrb.selectedInUse = true;
      if (isVectorized) {
        // TODO: relying everywhere on the magical constants and columns being together means ACID
        //       columns are going to be super hard to change in a backward compat manner. I can
        //       foresee someone cursing while refactoring all the magic for prefix schema changes.
        // Exclude the row column.
        int acidColCount = OrcInputFormat.getRootColumn(false) - 1;
        VectorizedRowBatch inputVrb = new VectorizedRowBatch(
            acidColCount + 1 + vrb.getDataColumnCount() );
        // By assumption, ACID columns are currently always in the beginning of the arrays.
        System.arraycopy(cvb.cols, 0, inputVrb.cols, 0, acidColCount);
        for (int ixInReadSet = acidColCount; ixInReadSet < cvb.cols.length; ++ixInReadSet) {
          int ixInVrb = includes.getPhysicalColumnIds().get(ixInReadSet);
          // TODO: should we create the batch from vrbctx, and reuse the vectors, like below? Future work.
          inputVrb.cols[ixInVrb] = cvb.cols[ixInReadSet];
        }
        inputVrb.size = cvb.size;
        // TODO: reuse between calls
        @SuppressWarnings("resource")
        VectorizedOrcAcidRowBatchReader acidReader = new VectorizedOrcAcidRowBatchReader(
            (OrcSplit)split, jobConf, Reporter.NULL, new AcidWrapper(inputVrb), rbCtx, true);
        acidReader.next(NullWritable.get(), vrb);
      } else {
         // TODO: WTF? The old code seems to just drop the ball here.
        throw new AssertionError("Unsupported mode");
      }
    } else {
      if (includes.getPhysicalColumnIds().size() != cvb.cols.length) {
        throw new RuntimeException("Unexpected number of columns, VRB has "
            + includes.getPhysicalColumnIds().size() + " included, but the reader returned "
            + cvb.cols.length);
      }
      // VRB was created from VrbCtx, so we already have pre-allocated column vectors.
      // Return old CVs (if any) to caller. We assume these things all have the same schema.
      for (int ixInReadSet = 0; ixInReadSet < cvb.cols.length; ++ixInReadSet) {
        int ixInVrb = includes.getPhysicalColumnIds().get(ixInReadSet);
        cvb.swapColumnVector(ixInReadSet, vrb.cols, ixInVrb);
      }
      vrb.selectedInUse = false;
      vrb.size = cvb.size;
    }

    if (wasFirst) {
      firstReturnTime = counters.startTimeCounter();
    }
    return true;
  }

  public VectorizedRowBatchCtx getVectorizedRowBatchCtx() {
    return rbCtx;
  }

  private static final class AcidWrapper
      implements RecordReader<NullWritable, VectorizedRowBatch> {
    private final VectorizedRowBatch acidVrb;

    private AcidWrapper(VectorizedRowBatch acidVrb) {
      this.acidVrb = acidVrb;
    }

    @Override
    public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
      return true;
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public VectorizedRowBatch createValue() {
      return acidVrb;
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }
  }

  private final class IOUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(final Thread t, final Throwable e) {
      LlapIoImpl.LOG.error("Unhandled error from reader thread. threadName: {} threadId: {}" +
          " Message: {}", t.getName(), t.getId(), e.getMessage());
      try {
        setError(e);
      } catch (InterruptedException e1) {
        LOG.info("IOUncaughtExceptionHandler interrupted; ignoring");
      }
    }
  }

  ColumnVectorBatch nextCvb() throws InterruptedException, IOException {
    boolean isFirst = (lastCvb == null);
    if (!isFirst) {
      feedback.returnData(lastCvb);
    }

    // We are waiting for next block. Either we will get it, or be told we are done.
    int queueSize = queue.size();
    maxQueueSize = Math.max(queueSize, maxQueueSize);
    boolean doLogBlocking = LlapIoImpl.LOG.isTraceEnabled() && queueSize == 0;
    if (doLogBlocking) {
      LlapIoImpl.LOG.trace("next will block");
    }
    // We rely on the fact that poll() checks interrupt even when there's something in the queue.
    // If the structure is replaced with smth that doesn't, we MUST check interrupt here because
    // Hive operators rely on recordreader to handle task interruption, and unlike most RRs we
    // do not do any blocking IO ops on this thread.
    Object next = null;
    do {
      rethrowErrorIfAny(pendingError.get()); // Best-effort check; see the comment in the method.
      next = queue.poll(100, TimeUnit.MILLISECONDS);
    } while (next == null);
    if (doLogBlocking) {
      LlapIoImpl.LOG.trace("next is unblocked");
    }
    if (next == DONE_OBJECT) {
      return null; // We are done.
    }
    if (next instanceof Throwable) {
      rethrowErrorIfAny((Throwable) next);
      throw new AssertionError("Unreachable");
    }
    lastCvb = (ColumnVectorBatch) next;
    if (LlapIoImpl.LOG.isTraceEnabled()) {
      LlapIoImpl.LOG.trace("Processing will receive vector {}", lastCvb);
    }
    return lastCvb;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public VectorizedRowBatch createValue() {
    return rbCtx.createVectorizedRowBatch();
  }

  @Override
  public long getPos() throws IOException {
    return -1; // Position doesn't make sense for async reader, chunk order is arbitrary.
  }

  @Override
  public void close() throws IOException {
    if (LlapIoImpl.LOG.isTraceEnabled()) {
      LlapIoImpl.LOG.trace("close called; closed {}, interrupted {}, err {}, pending {}",
          isClosed, isInterrupted, pendingError.get(), queue.size());
    }
    LlapIoImpl.LOG.info("Maximum queue length observed " + maxQueueSize);
    LlapIoImpl.LOG.info("Llap counters: {}" ,counters); // This is where counters are logged!
    feedback.stop();
    isClosed = true;
    rethrowErrorIfAny(pendingError.get());
    MDC.clear();
  }

  private static void rethrowErrorIfAny(Throwable pendingError) throws IOException {
    // This is called either with an error that was queued, or an error that was set into the
    // atomic reference in this class. The latter is best-effort and is used to opportunistically
    // skip processing of a long queue when the error happens.
    if (pendingError == null) return;
    if (pendingError instanceof IOException) {
      throw (IOException)pendingError;
    }
    throw new IOException(pendingError);
  }

  @Override
  public void setDone() throws InterruptedException {
    if (LlapIoImpl.LOG.isDebugEnabled()) {
      LlapIoImpl.LOG.debug("setDone called; closed {}, interrupted {}, err {}, pending {}",
          isClosed, isInterrupted, pendingError.get(), queue.size());
    }
    enqueueInternal(DONE_OBJECT);
  }

  @Override
  public void consumeData(ColumnVectorBatch data) throws InterruptedException {
    if (LlapIoImpl.LOG.isTraceEnabled()) {
      LlapIoImpl.LOG.trace("consume called; closed {}, interrupted {}, err {}, pending {}",
          isClosed, isInterrupted, pendingError.get(), queue.size());
    }
    enqueueInternal(data);
  }

  @Override
  public void setError(Throwable t) throws InterruptedException {
    counters.incrCounter(LlapIOCounters.NUM_ERRORS);
    LlapIoImpl.LOG.debug("setError called; closed {}, interrupted {},  err {}, pending {}",
        isClosed, isInterrupted, pendingError.get(), queue.size());
    LlapIoImpl.LOG.warn("setError called with an error", t);
    assert t != null;
    pendingError.compareAndSet(null, t);
    enqueueInternal(t);
  }

  private void enqueueInternal(Object o) throws InterruptedException {
    // We need to loop here to handle the case where consumer goes away.
    do {} while (!isClosed && !isInterrupted && !queue.offer(o, 100, TimeUnit.MILLISECONDS));
  }

  @Override
  public float getProgress() throws IOException {
    // TODO: plumb progress info thru the reader if we can get metadata from loader first.
    return 0.0f;
  }

  
  /** This class encapsulates include-related logic for LLAP readers. It is not actually specific
   *  to LLAP IO but in LLAP IO in particular, I want to encapsulate all this mess for now until
   *  we have smth better like Schema Evolution v2. This can also hypothetically encapsulate
   *  field pruning inside structs and stuff like that. */
  private static class IncludesImpl implements SchemaEvolutionFactory, Includes {
    private List<Integer> readerLogicalColumnIds;
    private List<Integer> filePhysicalColumnIds;
    private Integer acidStructColumnId = null;

    // For current schema evolution.
    private TypeDescription readerSchema;
    private JobConf jobConf;

    public IncludesImpl(List<Integer> tableIncludedCols, boolean isAcidScan,
        VectorizedRowBatchCtx rbCtx, TypeDescription readerSchema, JobConf jobConf) {
          // Note: columnIds below makes additional changes for ACID. Don't use this var directly.
      this.readerSchema = readerSchema;
      this.jobConf = jobConf;
      if (tableIncludedCols == null) {
        // Assume including everything means the VRB will have everything.
        // TODO: this is rather brittle, esp. in view of schema evolution (in abstract, not as 
        //       currently implemented in Hive). The compile should supply the columns it expects
        //       to see, which is not "all, of any schema". Is VRB row CVs the right mechanism
        //       for that? Who knows. Perhaps resolve in schema evolution v2.
        tableIncludedCols = new ArrayList<>(rbCtx.getRowColumnTypeInfos().length);
        for (int i = 0; i < rbCtx.getRowColumnTypeInfos().length; ++i) {
          tableIncludedCols.add(i);
        }
      }
      LOG.debug("Logical table includes: {}", tableIncludedCols);
      this.readerLogicalColumnIds = tableIncludedCols;
      // Note: schema evolution currently does not support column index changes.
      //       So, the indices should line up... to be fixed in SE v2?
      List<Integer> filePhysicalColumnIds = readerLogicalColumnIds;
      if (isAcidScan) {
        int rootCol = OrcInputFormat.getRootColumn(false);
        filePhysicalColumnIds = new ArrayList<Integer>(filePhysicalColumnIds.size() + rootCol);
        this.acidStructColumnId = rootCol - 1; // OrcRecordUpdater.ROW. This is somewhat fragile...
        // Note: this guarantees that physical column IDs are in order.
        for (int i = 0; i < rootCol; ++i) {
          // We don't want to include the root struct in ACID case; it would cause the whole
          // struct to get read without projection.
          if (acidStructColumnId == i) continue;
          filePhysicalColumnIds.add(i);
        }
        for (int tableColumnId : readerLogicalColumnIds) {
          filePhysicalColumnIds.add(rootCol + tableColumnId);
        }
      }
 
      this.filePhysicalColumnIds = filePhysicalColumnIds;
    }

    @Override
    public String toString() {
      return "logical columns " + readerLogicalColumnIds
          + ", physical columns " + filePhysicalColumnIds;
    }

    @Override
    public SchemaEvolution createSchemaEvolution(TypeDescription fileSchema) {
      if (readerSchema == null) {
        readerSchema = fileSchema;
      }
      // TODO: will this work correctly with ACID?
      boolean[] readerIncludes = OrcInputFormat.genIncludedColumns(
          readerSchema, readerLogicalColumnIds);
      Reader.Options options = new Reader.Options(jobConf).include(readerIncludes);
      return new SchemaEvolution(fileSchema, readerSchema, options);
    }

    @Override
    public boolean[] generateFileIncludes(TypeDescription fileSchema) {
      return OrcInputFormat.genIncludedColumns(
          fileSchema, filePhysicalColumnIds, acidStructColumnId);
    }

    @Override
    public List<Integer> getPhysicalColumnIds() {
      return filePhysicalColumnIds;
    }

    @Override
    public List<Integer> getReaderLogicalColumnIds() {
      return readerLogicalColumnIds;
    }

    @Override
    public TypeDescription[] getBatchReaderTypes(TypeDescription fileSchema) {
      return OrcInputFormat.genIncludedTypes(
          fileSchema, filePhysicalColumnIds, acidStructColumnId);
    }
  }
} 
