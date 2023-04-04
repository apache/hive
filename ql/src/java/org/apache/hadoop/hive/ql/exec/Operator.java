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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.OpTraits;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.ql.stats.fs.FSStatsPublisher;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base operator implementation.
 **/
public abstract class Operator<T extends OperatorDesc> implements Serializable,Cloneable,
  Node {

  // Bean methods

  private static final long serialVersionUID = 1L;

  public static final String HIVE_COUNTER_CREATED_FILES = "CREATED_FILES";
  public static final String HIVE_COUNTER_CREATED_DYNAMIC_PARTITIONS = "CREATED_DYNAMIC_PARTITIONS";
  public static final String HIVE_COUNTER_FATAL = "FATAL_ERROR";
  public static final String CONTEXT_NAME_KEY = "__hive.context.name";

  private transient Configuration configuration;
  protected CompilationOpContext cContext;
  protected List<Operator<? extends OperatorDesc>> childOperators;
  protected List<Operator<? extends OperatorDesc>> parentOperators;
  protected String operatorId;
  protected final AtomicBoolean abortOp;
  private transient ExecMapperContext execContext;
  private transient boolean rootInitializeCalled = false;
  protected transient long numRows = 0;
  protected transient long runTimeNumRows = 0;
  private transient Configuration hconf;
  protected final transient Collection<Future<?>> asyncInitOperations = new HashSet<>();
  private String marker;

  // It can be optimized later so that an operator operator (init/close) is performed
  // only after that operation has been performed on all the parents. This will require
  // initializing the whole tree in all the mappers (which might be required for mappers
  // spanning multiple files anyway, in future)
  /**
   * State.
   *
   */
  public static enum State {
    UNINIT, // initialize() has not been called
    INIT, // initialize() has been called and close() has not been called,
    // or close() has been called but one of its parent is not closed.
    CLOSE
    // all its parents operators are in state CLOSE and called close()
    // to children. Note: close() being called and its state being CLOSE is
    // difference since close() could be called but state is not CLOSE if
    // one of its parent is not in state CLOSE..
  }

  /**
   * Counters.
   */
  public enum Counter {
    RECORDS_OUT_OPERATOR,
    RECORDS_OUT_INTERMEDIATE
  }

  protected transient State state = State.UNINIT;

  private boolean useBucketizedHiveInputFormat;

  // Data structures specific for vectorized operators.
  private transient boolean multiChildren;
  private transient int[] selected;

  // dummy operator (for not increasing seqId)
  protected Operator(String name, CompilationOpContext cContext) {
    this();
    this.cContext = cContext;
    this.id = name;
    initOperatorId();
  }

  /** Kryo ctor. */
  protected Operator() {
    childOperators = new ArrayList<Operator<? extends OperatorDesc>>();
    parentOperators = new ArrayList<Operator<? extends OperatorDesc>>();
    abortOp = new AtomicBoolean(false);
  }

  public Operator(CompilationOpContext cContext) {
    this(String.valueOf(cContext.nextOperatorId()), cContext);
  }

  public void setChildOperators(
      List<Operator<? extends OperatorDesc>> childOperators) {
    if (childOperators == null) {
      childOperators = new ArrayList<Operator<? extends OperatorDesc>>();
    }
    this.childOperators = childOperators;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public List<Operator<? extends OperatorDesc>> getChildOperators() {
    return childOperators;
  }

  public int getNumChild() {
    return childOperators == null ? 0 : childOperators.size();
  }

  /**
   * Implements the getChildren function for the Node Interface.
   */
  @Override
  public List<Node> getChildren() {
    List<Operator<? extends OperatorDesc>> childOps = getChildOperators();
    return (childOps == null) ? null : new ArrayList<>(childOps);
  }

  public void setParentOperators(
      List<Operator<? extends OperatorDesc>> parentOperators) {
    if (parentOperators == null) {
      parentOperators = new ArrayList<Operator<? extends OperatorDesc>>();
    }
    this.parentOperators = parentOperators;
  }

  public List<Operator<? extends OperatorDesc>> getParentOperators() {
    return parentOperators;
  }

  public int getNumParent() {
    return parentOperators == null ? 0 : parentOperators.size();
  }

  protected T conf;
  protected boolean done;

  public void setConf(T conf) {
    this.conf = conf;
  }

  @Explain
  public T getConf() {
    return conf;
  }

  public boolean getDone() {
    return done;
  }

  protected final void setDone(boolean done) {
    this.done = done;
  }

  // non-bean fields needed during compilation
  private RowSchema rowSchema;

  public void setSchema(RowSchema rowSchema) {
    this.rowSchema = rowSchema;
  }

  public RowSchema getSchema() {
    return rowSchema;
  }

  // non-bean ..

  protected transient Map<String, LongWritable> statsMap = new HashMap<String, LongWritable>();
  @SuppressWarnings("rawtypes")
  protected transient OutputCollector out;
  protected transient final Logger LOG = LoggerFactory.getLogger(getClass().getName());
  protected transient String alias;
  protected transient Reporter reporter;
  protected String id;
  // object inspectors for input rows
  // We will increase the size of the array on demand
  protected transient ObjectInspector[] inputObjInspectors = new ObjectInspector[1];
  // for output rows of this operator
  protected transient ObjectInspector outputObjInspector;

  /**
   * This function is not named getId(), to make sure java serialization does
   * NOT serialize it. Some TestParse tests will fail if we serialize this
   * field, since the Operator ID will change based on the number of query
   * tests.
   */
  public String getIdentifier() {
    return id;
  }

  public void setReporter(Reporter rep) {
    reporter = rep;

    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.setReporter(rep);
    }
  }

  @SuppressWarnings("rawtypes")
  public void setOutputCollector(OutputCollector out) {
    this.out = out;

    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.setOutputCollector(out);
    }
  }

  /**
   * Store the alias this operator is working on behalf of.
   */
  public void setAlias(String alias) {
    this.alias = alias;

    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.setAlias(alias);
    }
  }

  public Map<String, Long> getStats() {
    HashMap<String, Long> ret = new HashMap<String, Long>();
    for (String one : statsMap.keySet()) {
      ret.put(one, Long.valueOf(statsMap.get(one).get()));
    }
    return (ret);
  }

  /**
   * checks whether all parent operators are initialized or not.
   *
   * @return true if there are no parents or all parents are initialized. false
   *         otherwise
   */
  protected boolean areAllParentsInitialized() {
    for (Operator<? extends OperatorDesc> parent : parentOperators) {
      if (parent != null && parent.state != State.INIT) {
        return false;
      }
    }
    return true;
  }

  /**
   * Initializes operators only if all parents have been initialized. Calls
   * operator specific initializer which then initializes child ops.
   *
   * @param hconf
   * @param inputOIs
   *          input object inspector array indexes by tag id. null value is
   *          ignored.
   * @throws HiveException
   */
  @SuppressWarnings("unchecked")
  public final void initialize(Configuration hconf, ObjectInspector[] inputOIs)
      throws HiveException {

    this.done = false;
    this.runTimeNumRows = 0; // initializeOp can be overridden
    // Initializing data structures for vectorForward
    this.selected = new int[VectorizedRowBatch.DEFAULT_SIZE];
    if (state == State.INIT) {
      return;
    }

    this.configuration = hconf;
    if (!areAllParentsInitialized()) {
      return;
    }

    LOG.info("Initializing Operator: {}", this);

    if (inputOIs != null) {
      inputObjInspectors = inputOIs;
    }

    // while initializing so this need to be done here instead of constructor
    childOperatorsArray = new Operator[childOperators.size()];
    for (int i = 0; i < childOperatorsArray.length; i++) {
      childOperatorsArray[i] = childOperators.get(i);
    }
    multiChildren = childOperatorsArray.length > 1;
    childOperatorsTag = new int[childOperatorsArray.length];
    for (int i = 0; i < childOperatorsArray.length; i++) {
      List<Operator<? extends OperatorDesc>> parentOperators =
          childOperatorsArray[i].getParentOperators();
      childOperatorsTag[i] = parentOperators.indexOf(this);
      if (childOperatorsTag[i] == -1) {
        throw new HiveException("Hive internal error: cannot find parent in the child operator!");
      }
    }

    if (inputObjInspectors.length == 0) {
      throw new HiveException("Internal Error during operator initialization.");
    }

    // derived classes can set this to different object if needed
    outputObjInspector = inputObjInspectors[0];

    boolean isInitOk = false;
    try {
      initializeOp(hconf);
      // sanity checks
      if (!rootInitializeCalled
          || childOperatorsArray.length != childOperators.size()) {
        throw new AssertionError("Internal error during operator initialization");
      }

      LOG.debug("Initialization Done: {}", this);

      initializeChildren(hconf);
      isInitOk = true;
    } finally {
      // TODO: ugly hack because Java does not have dtors and Tez input
      // hangs on shutdown.
      if (!isInitOk) {
        cancelAsyncInitOps();
      }
    }

    LOG.debug("Initialization Done - Reset: {}", this);

    // let's wait on the async ops before continuing
    completeInitialization(asyncInitOperations);
  }

  private void cancelAsyncInitOps() {
    for (Future<?> f : asyncInitOperations) {
      f.cancel(true);
    }
    asyncInitOperations.clear();
  }

  private void completeInitialization(Collection<Future<?>> fs) throws HiveException {
    Object[] os = new Object[fs.size()];
    int i = 0;
    Throwable asyncEx = null;

    // Wait for all futures to complete. Check for an abort while waiting for each future. If any of the futures is cancelled / aborted - cancel all subsequent futures.

    boolean cancelAll = false;
    for (Future<?> f : fs) {
      // If aborted - break out of the loop, and cancel all subsequent futures.
      if (cancelAll) {
        break;
      }
      if (abortOp.get()) {
        cancelAll = true;
        break;
      } else {
        // Wait for the current future.
        while (true) {
          if (abortOp.get()) {
            cancelAll = true;
            break;
          } else {
            try {
              // Await future result with a timeout to check the abort field occasionally.
              // It's possible that the interrupt which comes in along with an abort, is suppressed
              // by some other operator.
              Object futureResult = f.get(200l, TimeUnit.MILLISECONDS);
              os[i++] = futureResult;
              break;
            } catch (TimeoutException e) {
              // Expected if the operation takes time. Continue the loop, and wait for op completion.
            } catch (InterruptedException | CancellationException e) {
              asyncEx = e;
              cancelAll = true;
              break;
            } catch (ExecutionException e) {
              if (e.getCause() == null) {
                asyncEx = e;
              } else {
                asyncEx = e.getCause();
              }
              cancelAll = true;
              break;
            }
          }
        }
      }
    }

    if (cancelAll || asyncEx != null) {
      for (Future<?> f : fs) {
        // It's ok to send a cancel to an already completed future. Is a no-op
        f.cancel(true);
      }
      throw new HiveException("Async Initialization failed. abortRequested=" + abortOp.get(), asyncEx);
    }

    completeInitializationOp(os);
  }

  /**
   * This method can be used to retrieve the results from async operations
   * started at init time - before the operator pipeline is started.
   *
   * @param os
   * @throws HiveException
   */
  protected void completeInitializationOp(Object[] os) throws HiveException {
    // no-op default
  }

  public void initializeLocalWork(Configuration hconf) throws HiveException {
    if (CollectionUtils.isNotEmpty(childOperators)) {
      for (Operator<? extends OperatorDesc> childOp : childOperators) {
        childOp.initializeLocalWork(hconf);
      }
    }
  }

  /**
   * Operator specific initialization.
   */
  protected void initializeOp(Configuration hconf) throws HiveException {
    this.hconf = hconf;
    rootInitializeCalled = true;
  }

  public String getCounterName(Counter counter, Configuration hconf) {
    String context = hconf.get(Operator.CONTEXT_NAME_KEY, "");
    if (StringUtils.isNotEmpty(context)) {
      context = "_" + context.replace(" ", "_");
    }
    return counter + context;
  }

  /**
   * Calls initialize on each of the children with outputObjetInspector as the
   * output row format.
   */
  protected void initializeChildren(Configuration hconf) throws HiveException {
    state = State.INIT;
    LOG.debug("Operator Initialized: {}", this);

    if (CollectionUtils.isEmpty(childOperators)) {
      return;
    }

    LOG.debug("Initializing Children: {}", this);

    for (int i = 0; i < childOperatorsArray.length; i++) {
      childOperatorsArray[i].initialize(hconf, outputObjInspector, childOperatorsTag[i]);
      if (reporter != null) {
        childOperatorsArray[i].setReporter(reporter);
      }
    }
  }

  public void abort() {
    LOG.info("Received Abort in Operator: {}", this);
    abortOp.set(true);
  }

  /**
   * Pass the execContext reference to every child operator
   */
  public void passExecContext(ExecMapperContext execContext) {
    this.setExecContext(execContext);
    for (Operator<? extends OperatorDesc> childOp : childOperators) {
      childOp.passExecContext(execContext);
    }
  }

  /**
   * Collects all the parent's output object inspectors and calls actual
   * initialization method.
   *
   * @param hconf
   * @param inputOI
   *          OI of the row that this parent will pass to this op
   * @param parentId
   *          parent operator id
   * @throws HiveException
   */
  protected void initialize(Configuration hconf, ObjectInspector inputOI,
      int parentId) throws HiveException {
    LOG.debug("Initializing Child : {}", this);
    if (parentId >= inputObjInspectors.length) {
      // Determine the next power of 2 larger than the requested index
      int newLength = 2;
      while (parentId >= newLength) {
        newLength <<= 1;
      }
      inputObjInspectors = Arrays.copyOf(inputObjInspectors, newLength);
    }
    inputObjInspectors[parentId] = inputOI;
    // call the actual operator initialization function
    initialize(hconf, null);
  }

  public ObjectInspector[] getInputObjInspectors() {
    return inputObjInspectors;
  }

  public void setInputObjInspectors(ObjectInspector[] inputObjInspectors) {
    this.inputObjInspectors = inputObjInspectors;
  }

  public ObjectInspector getOutputObjInspector() {
    return outputObjInspector;
  }

  /**
   * Process the row.
   *
   * @param row
   *          The object representing the row.
   * @param tag
   *          The tag of the row usually means which parent this row comes from.
   *          Rows with the same tag should have exactly the same rowInspector
   *          all the time.
   */
  public abstract void process(Object row, int tag) throws HiveException;

  protected final void defaultStartGroup() throws HiveException {
    LOG.trace("Starting group");

    if (CollectionUtils.isEmpty(childOperators))  {
      LOG.trace("No children operators; start group done");
      return;
    }

    LOG.trace("Starting group for children");
    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.startGroup();
    }
    LOG.trace("Start group done");
  }

  protected final void defaultEndGroup() throws HiveException {
    LOG.trace("Ending group");

    if (CollectionUtils.isEmpty(childOperators)) {
      LOG.trace("No children operators; end group done");
      return;
    }

    LOG.trace("Ending group for children");
    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.endGroup();
    }
    LOG.trace("End group done");
  }

  // If a operator wants to do some work at the beginning of a group
  public void startGroup() throws HiveException {
    defaultStartGroup();
  }

  // If an operator wants to do some work at the end of a group
  public void endGroup() throws HiveException {
    defaultEndGroup();
  }

  // Tell the operator the status of the next key-grouped VectorizedRowBatch that will be delivered
  // to the process method.  E.g. by reduce-shuffle.  These semantics are needed by PTF so it can
  // efficiently add computed values to the last batch of a group key.
  public void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException {
    // Do nothing.
  }

  // an blocking operator (e.g. GroupByOperator and JoinOperator) can
  // override this method to forward its outputs
  public void flush() throws HiveException {
  }

  // Recursive flush to flush all the tree operators
  public void flushRecursive() throws HiveException {
    flush();
    if (CollectionUtils.isNotEmpty(childOperators)) {
      for (Operator<?> child : childOperators) {
        child.flushRecursive();
      }
    }
  }

  public void processGroup(int tag) throws HiveException {
    if (CollectionUtils.isNotEmpty(childOperators)) {
      for (int i = 0; i < childOperatorsArray.length; i++) {
        childOperatorsArray[i].processGroup(childOperatorsTag[i]);
      }
    }
  }

  protected boolean allInitializedParentsAreClosed() {
    if (parentOperators != null) {
      for (Operator<? extends OperatorDesc> parent : parentOperators) {
        if (parent == null) {
          continue;
        }
        LOG.debug("allInitializedParentsAreClosed? parent.state = {}",
            parent.state);
        if (!(parent.state == State.CLOSE || parent.state == State.UNINIT)) {
          return false;
        }
      }
    }
    return true;
  }

  // This close() function does not need to be synchronized
  // since it is called by its parents' main thread, so no
  // more than 1 thread should call this close() function.
  public void close(boolean abort) throws HiveException {
    LOG.debug("Close Called for Operator: {}", this);

    if (state == State.CLOSE) {
      return;
    }

    // check if all parents are finished
    if (!allInitializedParentsAreClosed()) {
      LOG.debug("Not all parent operators are closed. Not closing.");
      return;
    }

    // set state as CLOSE as long as all parents are closed
    // state == CLOSE doesn't mean all children are also in state CLOSE
    state = State.CLOSE;
    LOG.info("Closing Operator: {}", this);

    abort |= abortOp.get();

    // call the operator specific close routine
    closeOp(abort);

    // closeOp can be overriden
    if (conf != null && conf.getRuntimeStatsTmpDir() != null) {
      publishRunTimeStats();
    }
    LongWritable runTimeRowsWritable = new LongWritable(runTimeNumRows);
    LongWritable recordCounter = new LongWritable(numRows);
    statsMap.put(Counter.RECORDS_OUT_OPERATOR.name() + "_" + getOperatorId(), runTimeRowsWritable);
    statsMap.put(getCounterName(Counter.RECORDS_OUT_INTERMEDIATE, hconf), recordCounter);
    this.runTimeNumRows = 0;

    reporter = null;

    try {
      logStats();
      if (childOperators == null) {
        return;
      }

      for (Operator<? extends OperatorDesc> op : childOperators) {
        LOG.debug("Closing Child: {} ", op);
        op.close(abort);
      }

      LOG.debug("Close Done: {}", this);
    } catch (HiveException e) {
      LOG.warn("Caught exception while closing operator", e);
      throw e;
    }
  }

  /**
   * Operator specific close routine. Operators which inherents this class
   * should overwrite this funtion for their specific cleanup routine.
   */
  protected void closeOp(boolean abort) throws HiveException {
  }

  private boolean jobCloseDone = false;

  // Operator specific logic goes here
  public void jobCloseOp(Configuration conf, boolean success)
      throws HiveException {
  }

  /**
   * Unlike other operator interfaces which are called from map or reduce task,
   * jobClose is called from the jobclient side once the job has completed.
   *
   * @param conf
   *          Configuration with with which job was submitted
   * @param success
   *          whether the job was completed successfully or not
   */
  public void jobClose(Configuration conf, boolean success)
      throws HiveException {
    // JobClose has already been performed on this operator
    if (jobCloseDone) {
      return;
    }

    jobCloseOp(conf, success);
    jobCloseDone = true;

    if (CollectionUtils.isNotEmpty(this.childOperators)) {
      for (Operator<? extends OperatorDesc> op : this.childOperators) {
        op.jobClose(conf, success);
      }
    }
  }

  /**
   * Cache childOperators in an array for faster access. childOperatorsArray is
   * accessed per row, so it's important to make the access efficient.
   */
  protected transient Operator<? extends OperatorDesc>[] childOperatorsArray = null;
  protected transient int[] childOperatorsTag;

  /**
   * Replace one child with another at the same position. The parent of the
   * child is not changed
   *
   * @param child
   *          the old child
   * @param newChild
   *          the new child
   */
  public void replaceChild(Operator<? extends OperatorDesc> child,
      Operator<? extends OperatorDesc> newChild) {
    int childIndex = childOperators.indexOf(child);
    assert childIndex != -1;
    childOperators.set(childIndex, newChild);
  }

  public void removeChild(Operator<? extends OperatorDesc> child) {
    int childIndex = childOperators.indexOf(child);
    assert childIndex != -1;
    if (childOperators.size() == 1) {
      setChildOperators(null);
    } else {
      childOperators.remove(childIndex);
    }

    int parentIndex = child.getParentOperators().indexOf(this);
    assert parentIndex != -1;
    if (child.getParentOperators().size() == 1) {
      child.setParentOperators(null);
    } else {
      child.getParentOperators().remove(parentIndex);
    }
  }

  /**
   * Remove a child and add all of the child's children to the location of the child
   *
   * @param child   If this operator is not the only parent of the child. There can be unpredictable result.
   * @throws SemanticException
   */
  public void removeChildAndAdoptItsChildren(
    Operator<? extends OperatorDesc> child) throws SemanticException {
    int childIndex = childOperators.indexOf(child);
    if (childIndex == -1) {
      throw new SemanticException(
          "Exception when trying to remove partition predicates: fail to find child from parent");
    }

    childOperators.remove(childIndex);
    if (child.getChildOperators() != null &&
        child.getChildOperators().size() > 0) {
      childOperators.addAll(childIndex, child.getChildOperators());
    }

    for (Operator<? extends OperatorDesc> gc : child.getChildOperators()) {
      List<Operator<? extends OperatorDesc>> parents = gc.getParentOperators();
      int index = parents.indexOf(child);
      if (index == -1) {
        throw new SemanticException(
          "Exception when trying to remove partition predicates: fail to find parent from child");
      }
      parents.set(index, this);
    }
  }

  public void removeParent(Operator<? extends OperatorDesc> parent) {
    int parentIndex = parentOperators.indexOf(parent);
    assert parentIndex != -1;
    if (parentOperators.size() == 1) {
      setParentOperators(null);
    } else {
      parentOperators.remove(parentIndex);
    }

    int childIndex = parent.getChildOperators().indexOf(this);
    assert childIndex != -1;
    if (parent.getChildOperators().size() == 1) {
      parent.setChildOperators(null);
    } else {
      parent.getChildOperators().remove(childIndex);
    }
  }

  /**
   * Replace one parent with another at the same position. Chilren of the new
   * parent are not updated
   *
   * @param parent
   *          the old parent
   * @param newParent
   *          the new parent
   */
  public void replaceParent(Operator<? extends OperatorDesc> parent,
      Operator<? extends OperatorDesc> newParent) {
    int parentIndex = parentOperators.indexOf(parent);
    assert parentIndex != -1;
    parentOperators.set(parentIndex, newParent);
  }

  protected long getNextCntr(long cntr) {
    // A very simple counter to keep track of number of rows processed by an
    // operator. It dumps
    // every 1 million times, and quickly before that
    if (cntr >= 1000000) {
      return cntr + 1000000;
    }

    return 10 * cntr;
  }

  protected void forward(Object row, ObjectInspector rowInspector)
      throws HiveException {
    runTimeNumRows++;
    if (getDone()) {
      return;
    }

    int childrenDone = 0;
    for (int i = 0; i < childOperatorsArray.length; i++) {
      Operator<? extends OperatorDesc> o = childOperatorsArray[i];
      if (o.getDone()) {
        childrenDone++;
      } else {
        o.process(row, childOperatorsTag[i]);
      }
    }

    // if all children are done, this operator is also done
    if (childrenDone != 0 && childrenDone == childOperatorsArray.length) {
      setDone(true);
    }
  }

  /*
   * Forward a VectorizedRowBatch to the children operators.
   */
  protected void vectorForward(VectorizedRowBatch batch)
      throws HiveException {

    if (getDone()) {
      return;
    }

    // Data structures to store original values
    final int size = batch.size;
    runTimeNumRows += size;
    final boolean selectedInUse = batch.selectedInUse;
    final boolean saveState = (selectedInUse && multiChildren);
    if (saveState) {
      System.arraycopy(batch.selected, 0, selected, 0, size);
    }

    final int childSize = childOperatorsArray.length;
    if (childSize == 1) {
      childOperatorsArray[0].process(batch, childOperatorsTag[0]);
      // if that single child is done, this operator is also done
      if (childOperatorsArray[0].getDone()){
        setDone(true);
      }
    } else {
      int childrenDone = 0;
      for (int i = 0; i < childOperatorsArray.length; i++) {
        Operator<? extends OperatorDesc> o = childOperatorsArray[i];
        if (o.getDone()) {
          childrenDone++;
        } else {
          o.process(batch, childOperatorsTag[i]);

          // Restore original values
          batch.size = size;
          batch.selectedInUse = selectedInUse;
          if (saveState) {
            System.arraycopy(selected, 0, batch.selected, 0, size);
          }
        }
      }
      // if all children are done, this operator is also done
      if (childrenDone != 0 && childrenDone == childOperatorsArray.length) {
        setDone(true);
      }
    }
  }

  public void reset(){
    this.state = State.INIT;
    if (childOperators != null) {
      for (Operator<? extends OperatorDesc> o : childOperators) {
        o.reset();
      }
    }
  }

  /**
   * OperatorFunc.
   *
   */
  public static interface OperatorFunc {
    void func(Operator<? extends OperatorDesc> op);
  }

  public void preorderMap(OperatorFunc opFunc) {
    opFunc.func(this);
    if (childOperators != null) {
      for (Operator<? extends OperatorDesc> o : childOperators) {
        o.preorderMap(opFunc);
      }
    }
  }

  public void logStats() {
    if (LOG.isInfoEnabled() && !statsMap.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, LongWritable> e : statsMap.entrySet()) {
        sb.append(e.getKey()).append(":").append(e.getValue()).append(", ");
      }
      LOG.info(sb.toString());
    }
  }

  @Override
  public abstract String getName();

  static public String getOperatorName() {
    return "OP";
  }

  /**
   * Returns a map of output column name to input expression map Note that
   * currently it returns only key columns for ReduceSink and GroupBy operators.
   *
   * @return null if the operator doesn't change columns
   */
  public Map<String, ExprNodeDesc> getColumnExprMap() {
    if (this.getConf() == null) {
      return null;
    }
    return this.getConf().getColumnExprMap();
  }

  public void setColumnExprMap(Map<String, ExprNodeDesc> colExprMap) {
    if (this.getConf() != null) {
      this.getConf().setColumnExprMap(colExprMap);
    }
  }

  private String getLevelString(int level) {
    if (level == 0) {
      return "\n";
    }
    StringBuilder s = new StringBuilder();
    s.append("\n");
    while (level > 0) {
      s.append("  ");
      level--;
    }
    return s.toString();
  }

  public String dump(int level) {
    return dump(level, new HashSet<Integer>());
  }

  public String dump(int level, HashSet<Integer> seenOpts) {
    Integer idObj = Integer.valueOf(id);
    if (seenOpts.contains(idObj)) {
      return null;
    }
    seenOpts.add(idObj);

    StringBuilder s = new StringBuilder();
    String ls = getLevelString(level);
    s.append(ls);
    s.append("<" + getName() + ">");
    s.append("Id =" + id);

    if (childOperators != null) {
      s.append(ls);
      s.append("  <Children>");
      for (Operator<? extends OperatorDesc> o : childOperators) {
        s.append(o.dump(level + 2, seenOpts));
      }
      s.append(ls);
      s.append("  <\\Children>");
    }

    if (parentOperators != null) {
      s.append(ls);
      s.append("  <Parent>");
      for (Operator<? extends OperatorDesc> o : parentOperators) {
        s.append("Id = " + o.id + " ");
        s.append(o.dump(level, seenOpts));
      }
      s.append("<\\Parent>");
    }

    s.append(ls);
    s.append("<\\" + getName() + ">");
    return s.toString();
  }

  /**
   * Initialize an array of ExprNodeEvaluator and return the result
   * ObjectInspectors.
   */
  protected static ObjectInspector[] initEvaluators(ExprNodeEvaluator<?>[] evals,
      ObjectInspector rowInspector) throws HiveException {
    ObjectInspector[] result = new ObjectInspector[evals.length];
    for (int i = 0; i < evals.length; i++) {
      result[i] = evals[i].initialize(rowInspector);
    }
    return result;
  }

  /**
   * Initialize an array of ExprNodeEvaluator from start, for specified length
   * and return the result ObjectInspectors.
   */
  protected static ObjectInspector[] initEvaluators(ExprNodeEvaluator<?>[] evals,
      int start, int length,
      ObjectInspector rowInspector) throws HiveException {
    ObjectInspector[] result = new ObjectInspector[length];
    for (int i = 0; i < length; i++) {
      result[i] = evals[start + i].initialize(rowInspector);
    }
    return result;
  }

  /**
   * Initialize an array of ExprNodeEvaluator and put the return values into a
   * StructObjectInspector with integer field names.
   */
  protected static StructObjectInspector initEvaluatorsAndReturnStruct(
      ExprNodeEvaluator<?>[] evals, List<String> outputColName,
      ObjectInspector rowInspector) throws HiveException {
    ObjectInspector[] fieldObjectInspectors = initEvaluators(evals,
        rowInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        outputColName, Arrays.asList(fieldObjectInspectors));
  }

  protected transient Object groupKeyObject;

  public String getOperatorId() {
    return operatorId;
  }

  public String getMarker() {
    return marker;
  }

  public void setMarker(String marker) {
    this.marker = marker;
  }

  public void initOperatorId() {
    this.operatorId = getName() + '_' + this.id;
  }

  /*
   * By default, the list is empty - if an operator wants to add more counters,
   * it should override this method and provide the new list. Counter names returned
   * by this method should be wrapped counter names (i.e the strings should be passed
   * through getWrappedCounterName).
   */
  protected List<String> getAdditionalCounters() {
    return null;
  }

  /**
   * Return the type of the specific operator among the
   * types in OperatorType.
   *
   * @return OperatorType.*
   */
  abstract public OperatorType getType();

  public void setGroupKeyObject(Object keyObject) {
    this.groupKeyObject = keyObject;
  }

  public Object getGroupKeyObject() {
    return groupKeyObject;
  }

  /**
   * Called during semantic analysis as operators are being added
   * in order to give them a chance to compute any additional plan information
   * needed.  Does nothing by default.
   */
  public void augmentPlan() {
  }

  public ExecMapperContext getExecContext() {
    return execContext;
  }

  public void setExecContext(ExecMapperContext execContext) {
    this.execContext = execContext;
  }

  // The input file has changed - every operator can invoke specific action
  // for each input file
  public void cleanUpInputFileChanged() throws HiveException {
    this.cleanUpInputFileChangedOp();
    if (CollectionUtils.isNotEmpty(this.childOperators)) {
      for (Operator<? extends OperatorDesc> op : this.childOperators) {
        op.cleanUpInputFileChanged();
      }
    }
  }

  // If a operator needs to invoke specific cleanup, that operator can override
  // this method
  public void cleanUpInputFileChangedOp() throws HiveException {
  }

  // called by map operator. propagated recursively to single parented descendants
  public void setInputContext(String tableName, String partitionName) {
    if (childOperators != null) {
      for (Operator<? extends OperatorDesc> child : childOperators) {
        if (child.getNumParent() == 1) {
          child.setInputContext(tableName, partitionName);
        }
      }
    }
  }

  public boolean supportSkewJoinOptimization() {
    return false;
  }

  @Override
  public Operator<? extends OperatorDesc> clone()
    throws CloneNotSupportedException {

    List<Operator<? extends OperatorDesc>> parents = getParentOperators();
    List<Operator<? extends OperatorDesc>> parentClones =
      new ArrayList<Operator<? extends OperatorDesc>>();

    if (parents != null) {
      for (Operator<? extends OperatorDesc> parent : parents) {
        parentClones.add((parent.clone()));
      }
    }

    @SuppressWarnings("unchecked")
    T descClone = (T)conf.clone();
    // also clone the colExprMap by default
    // we need a deep copy
    ArrayList<ColumnInfo> colInfos =
        new ArrayList<>(getSchema().getSignature());
    Map<String, ExprNodeDesc> map = null;
    Map<String, ExprNodeDesc> colExprMap = getColumnExprMap();
    if (colExprMap != null) {
      map = new HashMap<>(colExprMap);
    }
    Operator<? extends OperatorDesc> ret = OperatorFactory.getAndMakeChild(
            cContext, descClone, new RowSchema(colInfos), map, parentClones);

    return ret;
  }

  /**
   * Clones only the operator. The children and parent are set
   * to null.
   * @return Cloned operator
   * @throws CloneNotSupportedException
   */
  @SuppressWarnings("unchecked")
  public Operator<? extends OperatorDesc> cloneOp() throws CloneNotSupportedException {
    T descClone = (T) conf.clone();
    Operator<? extends OperatorDesc> ret =
        OperatorFactory.getAndMakeChild(cContext, descClone, getSchema());
    return ret;
  }

  /**
   * Recursively clones all the children of the tree,
   * Fixes the pointers to children, parents and the pointers to itself coming from the children.
   * It does not fix the pointers to itself coming from parents, parents continue to point to
   * the original child.
   * @return Cloned operator
   * @throws CloneNotSupportedException
   */
  public Operator<? extends OperatorDesc> cloneRecursiveChildren()
      throws CloneNotSupportedException {
    Operator<? extends OperatorDesc> newOp = this.cloneOp();
    newOp.setParentOperators(this.parentOperators);
    List<Operator<? extends OperatorDesc>> newChildren =
        new ArrayList<Operator<? extends OperatorDesc>>();

    for (Operator<? extends OperatorDesc> childOp : this.getChildOperators()) {
      List<Operator<? extends OperatorDesc>> parentList =
          new ArrayList<Operator<? extends OperatorDesc>>();
      for (Operator<? extends OperatorDesc> parent : childOp.getParentOperators()) {
        if (parent.equals(this)) {
          parentList.add(newOp);
        } else {
          parentList.add(parent);
        }
      }
      // Recursively clone the children
      Operator<? extends OperatorDesc> clonedChildOp = childOp.cloneRecursiveChildren();
      clonedChildOp.setParentOperators(parentList);
    }

    newOp.setChildOperators(newChildren);
    return newOp;
  }


  /*
   * True only for operators which produce atmost 1 output row per input
   * row to it. This will allow the output column names to be directly
   * translated to input column names.
   */
  public boolean columnNamesRowResolvedCanBeObtained() {
    return false;
  }

  public boolean isUseBucketizedHiveInputFormat() {
    return useBucketizedHiveInputFormat;
  }

  /**
   * Before setting this to {@code true} make sure it's not reading ACID tables
   * @param useBucketizedHiveInputFormat
   */
  public void setUseBucketizedHiveInputFormat(boolean useBucketizedHiveInputFormat) {
    this.useBucketizedHiveInputFormat = useBucketizedHiveInputFormat;
  }

  /**
   * Whether this operator supports automatic sort merge join.
   * The stack is traversed, and this method is invoked for all the operators.
   * @return TRUE if yes, FALSE otherwise.
   */
  public boolean supportAutomaticSortMergeJoin() {
    return false;
  }

  public boolean supportUnionRemoveOptimization() {
    return false;
  }

  /*
   * This operator is allowed before mapjoin. Eventually, mapjoin hint should be done away with.
   * But, since bucketized mapjoin and sortmerge join depend on it completely. it is needed.
   * Check the operators which are allowed before mapjoin.
   */
  public boolean opAllowedBeforeMapJoin() {
    return true;
  }

  /*
   * This operator is allowed after mapjoin. Eventually, mapjoin hint should be done away with.
   * But, since bucketized mapjoin and sortmerge join depend on it completely. it is needed.
   * Check the operators which are allowed after mapjoin.
   */
  public boolean opAllowedAfterMapJoin() {
    return true;
  }

  /*
   * If this task contains a join, it can be converted to a map-join task if this operator is
   * present in the mapper. For eg. if a sort-merge join operator is present followed by a regular
   * join, it cannot be converted to a auto map-join.
   */
  public boolean opAllowedConvertMapJoin() {
    return true;
  }

  /*
   * If this task contains a sortmergejoin, it can be converted to a map-join task if this operator
   * is present in the mapper. For eg. if a sort-merge join operator is present followed by a
   * regular join, it cannot be converted to a auto map-join.
   */
  public boolean opAllowedBeforeSortMergeJoin() {
    return true;
  }

  /**
   * used for LimitPushdownOptimizer
   *
   * if all of the operators between limit and reduce-sink does not remove any input rows
   * in the range of limit count, limit can be pushed down to reduce-sink operator.
   * forward, select, etc.
   */
  public boolean acceptLimitPushdown() {
    return false;
  }

  @Override
  public String toString() {
    return getName() + '[' + getIdentifier() + ']';
  }

  public static String toString(Collection<TableScanOperator> top) {
    StringBuilder builder = new StringBuilder();
    Set<String> visited = new HashSet<String>();
    for (Operator<?> op : top) {
      if (builder.length() > 0) {
        builder.append('\n');
      }
      toString(builder, visited, op, 0);
    }
    return builder.toString();
  }

  public static String toString(Operator<?> op) {
    StringBuilder builder = new StringBuilder();
    toString(builder, new HashSet<String>(), op, 0);
    return builder.toString();
  }

  static boolean toString(StringBuilder builder, Set<String> visited, Operator<?> op, int start) {
    String name = op.toString();
    boolean added = visited.add(name);
    if (start > 0) {
      builder.append("-");
      start++;
    }
    builder.append(name);
    start += name.length();
    if (added) {
      if (op.getNumChild() > 0) {
        List<Operator<?>> children = op.getChildOperators();
        for (int i = 0; i < children.size(); i++) {
          if (i > 0) {
            builder.append('\n');
            for (int j = 0; j < start; j++) {
              builder.append(' ');
            }
          }
          toString(builder, visited, children.get(i), start);
        }
      }
      return true;
    }
    return false;
  }

  public Statistics getStatistics() {
    return (conf == null) ? null : conf.getStatistics();
  }

  public OpTraits getOpTraits() {
    return (conf == null) ? null : conf.getTraits();
  }

  public void setOpTraits(OpTraits metaInfo) {
    LOG.debug("Setting traits ({}) on: {}", metaInfo, this);
    if (conf != null) {
      conf.setTraits(metaInfo);
    } else {
      LOG.warn("Cannot set traits when there is no descriptor: {}", this);
    }
  }

  public void setStatistics(Statistics stats) {
    LOG.debug("Setting stats ({}) on: {}", stats, this);
    if (conf != null) {
      conf.setStatistics(stats);
    } else {
      LOG.warn("Cannot set stats when there is no descriptor: {}", this);
    }
  }

  @SuppressWarnings("rawtypes")
  public static Operator createDummy() {
    return new DummyOperator();
  }

  @SuppressWarnings({ "serial", "unchecked", "rawtypes" })
  private static class DummyOperator extends Operator {
    public DummyOperator() { super("dummy", null); }

    @Override
    public void process(Object row, int tag) {
    }

    @Override
    public OperatorType getType() {
      return null;
    }

    @Override
    public String getName() {
      return DummyOperator.getOperatorName();
    }

    public static String getOperatorName() {
      return "DUMMY";
    }

    @Override
    protected void initializeOp(Configuration conf) {
    }
  }

  public void removeParents() {
    for (Operator<?> parent : new ArrayList<Operator<?>>(getParentOperators())) {
      removeParent(parent);
    }
  }

  public boolean getIsReduceSink() {
    return false;
  }

  public String getReduceOutputName() {
    return null;
  }

  public void setCompilationOpContext(CompilationOpContext ctx) {
    if (cContext == ctx) {
      return;
    }
    cContext = ctx;
    id = String.valueOf(ctx.nextOperatorId());
    initOperatorId();
  }

  /** @return Compilation operator context. Only available during compilation. */
  public CompilationOpContext getCompilationOpContext() {
    return cContext;
  }

  private void publishRunTimeStats() throws HiveException {
    StatsPublisher statsPublisher = new FSStatsPublisher();
    StatsCollectionContext sContext = new StatsCollectionContext(hconf);
    sContext.setStatsTmpDir(conf.getRuntimeStatsTmpDir());
    sContext.setContextSuffix(getOperatorId());

    if (!statsPublisher.connect(sContext)) {
      LOG.error("StatsPublishing error: cannot connect to database");
      throw new HiveException(ErrorMsg.STATSPUBLISHER_CONNECTION_ERROR.getErrorCodedMsg());
    }

    String prefix = "";
    Map<String, String> statsToPublish = new HashMap<String, String>();
    statsToPublish.put(StatsSetupConst.RUN_TIME_ROW_COUNT, Long.toString(runTimeNumRows));
    if (!statsPublisher.publishStat(prefix, statsToPublish)) {
      // The original exception is lost.
      // Not changing the interface to maintain backward compatibility
      throw new HiveException(ErrorMsg.STATSPUBLISHER_PUBLISHING_ERROR.getErrorCodedMsg());
    }
    if (!statsPublisher.closeConnection(sContext)) {
      // The original exception is lost.
      // Not changing the interface to maintain backward compatibility
      throw new HiveException(ErrorMsg.STATSPUBLISHER_CLOSING_ERROR.getErrorCodedMsg());
    }
  }

  /**
   * Decides whether two operators are logically the same.
   * This can be used to merge same operators and avoid repeated computation.
   */
  public boolean logicalEquals(Operator other) {
    return getClass().getName().equals(other.getClass().getName()) &&
        (conf == other.getConf() || (conf != null && other.getConf() != null &&
            conf.isSame(other.getConf())));
  }

  /**
   * Compares the whole operator tree with the other.
   */
  // Currently only used during re-optimization related parts.
  // FIXME: HIVE-18703 should probably move this method somewhere else
  public final boolean logicalEqualsTree(Operator<?> o) {
    // XXX: this could easily become a hot-spot
    if (!logicalEquals(o)) {
      return false;
    }
    if (o.getNumParent() != getNumParent()) {
      return false;
    }
    for (int i = 0; i < getNumParent(); i++) {
      Operator<? extends OperatorDesc> copL = parentOperators.get(i);
      Operator<? extends OperatorDesc> copR = o.parentOperators.get(i);
      if (!copL.logicalEquals(copR)) {
        return false;
      }
    }
    return true;
  }

  public void replaceTabAlias(String oldAlias, String newAlias) {
    ExprNodeDescUtils.replaceTabAlias(getConf().getColumnExprMap(), oldAlias, newAlias);
    for (Operator<? extends OperatorDesc> c : getChildOperators()) {
      c.replaceTabAlias(oldAlias, newAlias);
    }
  }

  /**
   * There are aggregate implementations where the contents of the batch are processed in an async way,
   * (e.g. in executors/thread pool), in which cases we have to create a new batch every time. This leads to
   * minor GC pressure, so this feature should be only used when you have the evidence that there is an expression
   * which relies on this, and its implementation has benefits over the default, non-cloned approach.
   */
  public boolean batchNeedsClone() {
    return false;
  }
}
