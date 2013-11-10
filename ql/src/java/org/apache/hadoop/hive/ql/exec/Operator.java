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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;

/**
 * Base operator implementation.
 **/
public abstract class Operator<T extends OperatorDesc> implements Serializable,Cloneable,
  Node {

  // Bean methods

  private static final long serialVersionUID = 1L;

  private transient Configuration configuration;
  protected List<Operator<? extends OperatorDesc>> childOperators;
  protected List<Operator<? extends OperatorDesc>> parentOperators;
  protected String operatorId;
  /**
   * List of counter names associated with the operator. It contains the
   * following default counters NUM_INPUT_ROWS NUM_OUTPUT_ROWS TIME_TAKEN
   * Individual operators can add to this list via addToCounterNames methods.
   */
  protected ArrayList<String> counterNames;

  /**
   * Each operator has its own map of its counter names to disjoint
   * ProgressCounter - it is populated at compile time and is read in at
   * run-time while extracting the operator specific counts.
   */
  protected HashMap<String, ProgressCounter> counterNameToEnum;

  private transient ExecMapperContext execContext;

  private static AtomicInteger seqId;

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
  };

  protected transient State state = State.UNINIT;

  protected static transient boolean fatalError = false; // fatalError is shared acorss
  // all operators

  static {
    seqId = new AtomicInteger(0);
  }

  private boolean useBucketizedHiveInputFormat;

  public Operator() {
    id = String.valueOf(seqId.getAndIncrement());
  }

  public static void resetId() {
    seqId.set(0);
  }

  /**
   * Create an operator with a reporter.
   *
   * @param reporter
   *          Used to report progress of certain operators.
   */
  public Operator(Reporter reporter) {
    this();
    this.reporter = reporter;
  }

  public void setChildOperators(
      List<Operator<? extends OperatorDesc>> childOperators) {
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
  public ArrayList<Node> getChildren() {

    if (getChildOperators() == null) {
      return null;
    }

    ArrayList<Node> ret_vec = new ArrayList<Node>();
    for (Operator<? extends OperatorDesc> op : getChildOperators()) {
      ret_vec.add(op);
    }

    return ret_vec;
  }

  public void setParentOperators(
      List<Operator<? extends OperatorDesc>> parentOperators) {
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
    return done || fatalError;
  }

  public void setDone(boolean done) {
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

  protected transient HashMap<Enum<?>, LongWritable> statsMap = new HashMap<Enum<?>, LongWritable>();
  protected transient Log LOG = LogFactory.getLog(this.getClass().getName());
  protected transient boolean isLogInfoEnabled = LOG.isInfoEnabled();
  protected transient String alias;
  protected transient Reporter reporter;
  protected transient String id;
  // object inspectors for input rows
  // We will increase the size of the array on demand
  protected transient ObjectInspector[] inputObjInspectors = new ObjectInspector[1];
  // for output rows of this operator
  protected transient ObjectInspector outputObjInspector;

  /**
   * A map of output column name to input expression map. This is used by
   * optimizer and built during semantic analysis contains only key elements for
   * reduce sink and group by op
   */
  protected Map<String, ExprNodeDesc> colExprMap;

  public void setId(String id) {
    this.id = id;
  }

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

    // the collector is same across all operators
    if (childOperators == null) {
      return;
    }

    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.setReporter(rep);
    }
  }

  /**
   * Store the alias this operator is working on behalf of.
   */
  public void setAlias(String alias) {
    this.alias = alias;

    if (childOperators == null) {
      return;
    }

    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.setAlias(alias);
    }
  }

  public Map<Enum<?>, Long> getStats() {
    HashMap<Enum<?>, Long> ret = new HashMap<Enum<?>, Long>();
    for (Enum<?> one : statsMap.keySet()) {
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
    if (parentOperators == null) {
      return true;
    }
    for (Operator<? extends OperatorDesc> parent : parentOperators) {
      if (parent == null) {
        //return true;
        continue;
      }
      if (parent.state != State.INIT) {
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
  public void initialize(Configuration hconf, ObjectInspector[] inputOIs)
      throws HiveException {
    if (state == State.INIT) {
      return;
    }

    this.configuration = hconf;
    if (!areAllParentsInitialized()) {
      return;
    }

    LOG.info("Initializing Self " + id + " " + getName());

    if (inputOIs != null) {
      inputObjInspectors = inputOIs;
    }

    // initialize structure to maintain child op info. operator tree changes
    // while
    // initializing so this need to be done here instead of initialize() method
    if (childOperators != null) {
      childOperatorsArray = new Operator[childOperators.size()];
      for (int i = 0; i < childOperatorsArray.length; i++) {
        childOperatorsArray[i] = childOperators.get(i);
      }
      childOperatorsTag = new int[childOperatorsArray.length];
      for (int i = 0; i < childOperatorsArray.length; i++) {
        List<Operator<? extends OperatorDesc>> parentOperators = childOperatorsArray[i]
            .getParentOperators();
        if (parentOperators == null) {
          throw new HiveException("Hive internal error: parent is null in "
              + childOperatorsArray[i].getClass() + "!");
        }
        childOperatorsTag[i] = parentOperators.indexOf(this);
        if (childOperatorsTag[i] == -1) {
          throw new HiveException(
              "Hive internal error: cannot find parent in the child operator!");
        }
      }
    }

    if (inputObjInspectors.length == 0) {
      throw new HiveException("Internal Error during operator initialization.");
    }
    // derived classes can set this to different object if needed
    outputObjInspector = inputObjInspectors[0];

    //pass the exec context to child operators
    passExecContext(this.execContext);

    initializeOp(hconf);
    LOG.info("Initialization Done " + id + " " + getName());
  }

  public void initializeLocalWork(Configuration hconf) throws HiveException {
    if (childOperators != null) {
      for (int i =0; i<childOperators.size();i++) {
        Operator<? extends OperatorDesc> childOp = this.childOperators.get(i);
        childOp.initializeLocalWork(hconf);
      }
    }
  }

  /**
   * Operator specific initialization.
   */
  protected void initializeOp(Configuration hconf) throws HiveException {
    initializeChildren(hconf);
  }

  /**
   * Calls initialize on each of the children with outputObjetInspector as the
   * output row format.
   */
  protected void initializeChildren(Configuration hconf) throws HiveException {
    state = State.INIT;
    LOG.info("Operator " + id + " " + getName() + " initialized");
    if (childOperators == null) {
      return;
    }
    LOG.info("Initializing children of " + id + " " + getName());
    for (int i = 0; i < childOperatorsArray.length; i++) {
      childOperatorsArray[i].initialize(hconf, outputObjInspector,
          childOperatorsTag[i]);
      if (reporter != null) {
        childOperatorsArray[i].setReporter(reporter);
      }
    }
  }

  /**
   * Pass the execContext reference to every child operator
   */
  public void passExecContext(ExecMapperContext execContext) {
    this.setExecContext(execContext);
    if(childOperators != null) {
      for (int i = 0; i < childOperators.size(); i++) {
        childOperators.get(i).passExecContext(execContext);
      }
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
    LOG.info("Initializing child " + id + " " + getName());
    // Double the size of the array if needed
    if (parentId >= inputObjInspectors.length) {
      int newLength = inputObjInspectors.length * 2;
      while (parentId >= newLength) {
        newLength *= 2;
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
  public abstract void processOp(Object row, int tag) throws HiveException;

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
  public void process(Object row, int tag) throws HiveException {
    if (fatalError) {
      return;
    }

    if (counterNameToEnum != null) {
      inputRows++;
      if ((inputRows % 1000) == 0) {
        incrCounter(numInputRowsCntr, inputRows);
        incrCounter(timeTakenCntr, totalTime);
        inputRows = 0;
        totalTime = 0;
      }

      beginTime = System.currentTimeMillis();
      processOp(row, tag);
      totalTime += (System.currentTimeMillis() - beginTime);
    } else {
      processOp(row, tag);
    }
  }

  // If a operator wants to do some work at the beginning of a group
  public void startGroup() throws HiveException {
    LOG.debug("Starting group");

    if (childOperators == null) {
      return;
    }

    if (fatalError) {
      return;
    }

    LOG.debug("Starting group for children:");
    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.startGroup();
    }

    LOG.debug("Start group Done");
  }

  // If an operator wants to do some work at the end of a group
  public void endGroup() throws HiveException {
    LOG.debug("Ending group");

    if (childOperators == null) {
      return;
    }

    if (fatalError) {
      return;
    }

    LOG.debug("Ending group for children:");
    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.endGroup();
    }

    LOG.debug("End group Done");
  }

  // an blocking operator (e.g. GroupByOperator and JoinOperator) can
  // override this method to forward its outputs
  public void flush() throws HiveException {
  }

  public void processGroup(int tag) throws HiveException {
    if (childOperators == null) {
      return;
    }
    for (int i = 0; i < childOperatorsArray.length; i++) {
      childOperatorsArray[i].processGroup(childOperatorsTag[i]);
    }
  }

  protected boolean allInitializedParentsAreClosed() {
    if (parentOperators != null) {
      for (Operator<? extends OperatorDesc> parent : parentOperators) {
        if(parent==null){
          continue;
        }
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

    if (state == State.CLOSE) {
      return;
    }

    // check if all parents are finished
    if (!allInitializedParentsAreClosed()) {
      return;
    }

    // set state as CLOSE as long as all parents are closed
    // state == CLOSE doesn't mean all children are also in state CLOSE
    state = State.CLOSE;
    LOG.info(id + " finished. closing... ");

    // call the operator specific close routine
    closeOp(abort);

    if (counterNameToEnum != null) {
      incrCounter(numInputRowsCntr, inputRows);
      incrCounter(numOutputRowsCntr, outputRows);
      incrCounter(timeTakenCntr, totalTime);
    }

    LOG.info(id + " forwarded " + cntr + " rows");

    try {
      logStats();
      if (childOperators == null) {
        return;
      }

      for (Operator<? extends OperatorDesc> op : childOperators) {
        op.close(abort);
      }

      LOG.info(id + " Close done");
    } catch (HiveException e) {
      e.printStackTrace();
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
  public void jobCloseOp(Configuration conf, boolean success, JobCloseFeedBack feedBack)
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
  public void jobClose(Configuration conf, boolean success, JobCloseFeedBack feedBack)
      throws HiveException {
    // JobClose has already been performed on this operator
    if (jobCloseDone) {
      return;
    }

    jobCloseOp(conf, success, feedBack);
    jobCloseDone = true;

    if (childOperators != null) {
      for (Operator<? extends OperatorDesc> op : childOperators) {
        op.jobClose(conf, success, feedBack);
      }
    }
  }

  /**
   * Cache childOperators in an array for faster access. childOperatorsArray is
   * accessed per row, so it's important to make the access efficient.
   */
  protected transient Operator<? extends OperatorDesc>[] childOperatorsArray = null;
  protected transient int[] childOperatorsTag;

  // counters for debugging
  private transient long cntr = 0;
  private transient long nextCntr = 1;

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
      childOperators = null;
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
      parentOperators = null;
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

  // Remove the operators till a certain depth.
  // Return true if the remove was successful, false otherwise
  public boolean removeChildren(int depth) {
    Operator<? extends OperatorDesc> currOp = this;
    for (int i = 0; i < depth; i++) {
      // If there are more than 1 children at any level, don't do anything
      if ((currOp.getChildOperators() == null) ||
          (currOp.getChildOperators().size() > 1)) {
        return false;
      }
      currOp = currOp.getChildOperators().get(0);
    }

    setChildOperators(currOp.getChildOperators());

    List<Operator<? extends OperatorDesc>> parentOps =
      new ArrayList<Operator<? extends OperatorDesc>>();
    parentOps.add(this);

    for (Operator<? extends OperatorDesc> op : currOp.getChildOperators()) {
      op.setParentOperators(parentOps);
    }
    return true;
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

    if (counterNameToEnum != null) {
      if ((++outputRows % 1000) == 0) {
        incrCounter(numOutputRowsCntr, outputRows);
        outputRows = 0;
      }
    }

    increaseForward(1);

    // For debugging purposes:
    // System.out.println("" + this.getClass() + ": " +
    // SerDeUtils.getJSONString(row, rowInspector));
    // System.out.println("" + this.getClass() + ">> " +
    // ObjectInspectorUtils.getObjectInspectorName(rowInspector));

    if (childOperatorsArray == null && childOperators != null) {
      throw new HiveException(
          "Internal Hive error during operator initialization.");
    }

    if ((childOperatorsArray == null) || (getDone())) {
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
    if (childrenDone == childOperatorsArray.length) {
      setDone(true);
    }
  }

  void increaseForward(long counter) {
    if (isLogInfoEnabled) {
      cntr += counter;
      if (cntr >= nextCntr) {
        LOG.info(id + " forwarding " + cntr + " rows");
        do {
          nextCntr = getNextCntr(nextCntr);
        } while(cntr >= nextCntr);
      }
    }
  }

  public void resetStats() {
    for (Enum<?> e : statsMap.keySet()) {
      statsMap.get(e).set(0L);
    }
  }

  public void reset(){
    this.state=State.INIT;
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
    for (Enum<?> e : statsMap.keySet()) {
      LOG.info(e.toString() + ":" + statsMap.get(e).toString());
    }
  }

  /**
   * Implements the getName function for the Node Interface.
   *
   * @return the name of the operator
   */
  public String getName() {
    return getOperatorName();
  }

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
    return colExprMap;
  }

  public void setColumnExprMap(Map<String, ExprNodeDesc> colExprMap) {
    this.colExprMap = colExprMap;
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
    if (seenOpts.contains(new Integer(id))) {
      return null;
    }
    seenOpts.add(new Integer(id));

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
  protected static ObjectInspector[] initEvaluators(ExprNodeEvaluator[] evals,
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
  protected static ObjectInspector[] initEvaluators(ExprNodeEvaluator[] evals,
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
      ExprNodeEvaluator[] evals, List<String> outputColName,
      ObjectInspector rowInspector) throws HiveException {
    ObjectInspector[] fieldObjectInspectors = initEvaluators(evals,
        rowInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        outputColName, Arrays.asList(fieldObjectInspectors));
  }

  /**
   * All counter stuff below this
   */

  /**
   * TODO This is a hack for hadoop 0.17 which only supports enum counters.
   */
  public static enum ProgressCounter {
    CREATED_FILES,
    C1, C2, C3, C4, C5, C6, C7, C8, C9, C10,
    C11, C12, C13, C14, C15, C16, C17, C18, C19, C20,
    C21, C22, C23, C24, C25, C26, C27, C28, C29, C30,
    C31, C32, C33, C34, C35, C36, C37, C38, C39, C40,
    C41, C42, C43, C44, C45, C46, C47, C48, C49, C50,
    C51, C52, C53, C54, C55, C56, C57, C58, C59, C60,
    C61, C62, C63, C64, C65, C66, C67, C68, C69, C70,
    C71, C72, C73, C74, C75, C76, C77, C78, C79, C80,
    C81, C82, C83, C84, C85, C86, C87, C88, C89, C90,
    C91, C92, C93, C94, C95, C96, C97, C98, C99, C100,
    C101, C102, C103, C104, C105, C106, C107, C108, C109, C110,
    C111, C112, C113, C114, C115, C116, C117, C118, C119, C120,
    C121, C122, C123, C124, C125, C126, C127, C128, C129, C130,
    C131, C132, C133, C134, C135, C136, C137, C138, C139, C140,
    C141, C142, C143, C144, C145, C146, C147, C148, C149, C150,
    C151, C152, C153, C154, C155, C156, C157, C158, C159, C160,
    C161, C162, C163, C164, C165, C166, C167, C168, C169, C170,
    C171, C172, C173, C174, C175, C176, C177, C178, C179, C180,
    C181, C182, C183, C184, C185, C186, C187, C188, C189, C190,
    C191, C192, C193, C194, C195, C196, C197, C198, C199, C200,
    C201, C202, C203, C204, C205, C206, C207, C208, C209, C210,
    C211, C212, C213, C214, C215, C216, C217, C218, C219, C220,
    C221, C222, C223, C224, C225, C226, C227, C228, C229, C230,
    C231, C232, C233, C234, C235, C236, C237, C238, C239, C240,
    C241, C242, C243, C244, C245, C246, C247, C248, C249, C250,
    C251, C252, C253, C254, C255, C256, C257, C258, C259, C260,
    C261, C262, C263, C264, C265, C266, C267, C268, C269, C270,
    C271, C272, C273, C274, C275, C276, C277, C278, C279, C280,
    C281, C282, C283, C284, C285, C286, C287, C288, C289, C290,
    C291, C292, C293, C294, C295, C296, C297, C298, C299, C300,
    C301, C302, C303, C304, C305, C306, C307, C308, C309, C310,
    C311, C312, C313, C314, C315, C316, C317, C318, C319, C320,
    C321, C322, C323, C324, C325, C326, C327, C328, C329, C330,
    C331, C332, C333, C334, C335, C336, C337, C338, C339, C340,
    C341, C342, C343, C344, C345, C346, C347, C348, C349, C350,
    C351, C352, C353, C354, C355, C356, C357, C358, C359, C360,
    C361, C362, C363, C364, C365, C366, C367, C368, C369, C370,
    C371, C372, C373, C374, C375, C376, C377, C378, C379, C380,
    C381, C382, C383, C384, C385, C386, C387, C388, C389, C390,
    C391, C392, C393, C394, C395, C396, C397, C398, C399, C400,
    C401, C402, C403, C404, C405, C406, C407, C408, C409, C410,
    C411, C412, C413, C414, C415, C416, C417, C418, C419, C420,
    C421, C422, C423, C424, C425, C426, C427, C428, C429, C430,
    C431, C432, C433, C434, C435, C436, C437, C438, C439, C440,
    C441, C442, C443, C444, C445, C446, C447, C448, C449, C450,
    C451, C452, C453, C454, C455, C456, C457, C458, C459, C460,
    C461, C462, C463, C464, C465, C466, C467, C468, C469, C470,
    C471, C472, C473, C474, C475, C476, C477, C478, C479, C480,
    C481, C482, C483, C484, C485, C486, C487, C488, C489, C490,
    C491, C492, C493, C494, C495, C496, C497, C498, C499, C500,
    C501, C502, C503, C504, C505, C506, C507, C508, C509, C510,
    C511, C512, C513, C514, C515, C516, C517, C518, C519, C520,
    C521, C522, C523, C524, C525, C526, C527, C528, C529, C530,
    C531, C532, C533, C534, C535, C536, C537, C538, C539, C540,
    C541, C542, C543, C544, C545, C546, C547, C548, C549, C550,
    C551, C552, C553, C554, C555, C556, C557, C558, C559, C560,
    C561, C562, C563, C564, C565, C566, C567, C568, C569, C570,
    C571, C572, C573, C574, C575, C576, C577, C578, C579, C580,
    C581, C582, C583, C584, C585, C586, C587, C588, C589, C590,
    C591, C592, C593, C594, C595, C596, C597, C598, C599, C600,
    C601, C602, C603, C604, C605, C606, C607, C608, C609, C610,
    C611, C612, C613, C614, C615, C616, C617, C618, C619, C620,
    C621, C622, C623, C624, C625, C626, C627, C628, C629, C630,
    C631, C632, C633, C634, C635, C636, C637, C638, C639, C640,
    C641, C642, C643, C644, C645, C646, C647, C648, C649, C650,
    C651, C652, C653, C654, C655, C656, C657, C658, C659, C660,
    C661, C662, C663, C664, C665, C666, C667, C668, C669, C670,
    C671, C672, C673, C674, C675, C676, C677, C678, C679, C680,
    C681, C682, C683, C684, C685, C686, C687, C688, C689, C690,
    C691, C692, C693, C694, C695, C696, C697, C698, C699, C700,
    C701, C702, C703, C704, C705, C706, C707, C708, C709, C710,
    C711, C712, C713, C714, C715, C716, C717, C718, C719, C720,
    C721, C722, C723, C724, C725, C726, C727, C728, C729, C730,
    C731, C732, C733, C734, C735, C736, C737, C738, C739, C740,
    C741, C742, C743, C744, C745, C746, C747, C748, C749, C750,
    C751, C752, C753, C754, C755, C756, C757, C758, C759, C760,
    C761, C762, C763, C764, C765, C766, C767, C768, C769, C770,
    C771, C772, C773, C774, C775, C776, C777, C778, C779, C780,
    C781, C782, C783, C784, C785, C786, C787, C788, C789, C790,
    C791, C792, C793, C794, C795, C796, C797, C798, C799, C800,
    C801, C802, C803, C804, C805, C806, C807, C808, C809, C810,
    C811, C812, C813, C814, C815, C816, C817, C818, C819, C820,
    C821, C822, C823, C824, C825, C826, C827, C828, C829, C830,
    C831, C832, C833, C834, C835, C836, C837, C838, C839, C840,
    C841, C842, C843, C844, C845, C846, C847, C848, C849, C850,
    C851, C852, C853, C854, C855, C856, C857, C858, C859, C860,
    C861, C862, C863, C864, C865, C866, C867, C868, C869, C870,
    C871, C872, C873, C874, C875, C876, C877, C878, C879, C880,
    C881, C882, C883, C884, C885, C886, C887, C888, C889, C890,
    C891, C892, C893, C894, C895, C896, C897, C898, C899, C900,
    C901, C902, C903, C904, C905, C906, C907, C908, C909, C910,
    C911, C912, C913, C914, C915, C916, C917, C918, C919, C920,
    C921, C922, C923, C924, C925, C926, C927, C928, C929, C930,
    C931, C932, C933, C934, C935, C936, C937, C938, C939, C940,
    C941, C942, C943, C944, C945, C946, C947, C948, C949, C950,
    C951, C952, C953, C954, C955, C956, C957, C958, C959, C960,
    C961, C962, C963, C964, C965, C966, C967, C968, C969, C970,
    C971, C972, C973, C974, C975, C976, C977, C978, C979, C980,
    C981, C982, C983, C984, C985, C986, C987, C988, C989, C990,
    C991, C992, C993, C994, C995, C996, C997, C998, C999, C1000
  };

  private static int totalNumCntrs = 1000;

  /**
   * populated at runtime from hadoop counters at run time in the client.
   */
  protected transient HashMap<String, Long> counters;

  /**
   * keeps track of unique ProgressCounter enums used this value is used at
   * compile time while assigning ProgressCounter enums to counter names.
   */
  private static int lastEnumUsed;

  protected transient long inputRows = 0;
  protected transient long outputRows = 0;
  protected transient long beginTime = 0;
  protected transient long totalTime = 0;

  protected transient Object groupKeyObject;

  /**
   * this is called in operators in map or reduce tasks.
   *
   * @param name
   * @param amount
   */
  protected void incrCounter(String name, long amount) {
    String counterName = getWrappedCounterName(name);
    ProgressCounter pc = counterNameToEnum.get(counterName);

    // Currently, we maintain fixed number of counters per plan - in case of a
    // bigger tree, we may run out of them
    if (pc == null) {
      LOG
          .warn("Using too many counters. Increase the total number of counters for "
          + counterName);
    } else if (reporter != null) {
      reporter.incrCounter(pc, amount);
    }
  }

  public ArrayList<String> getCounterNames() {
    return counterNames;
  }

  public void setCounterNames(ArrayList<String> counterNames) {
    this.counterNames = counterNames;
  }

  public String getOperatorId() {
    return operatorId;
  }

  public final String getWrappedCounterName(String ctrName) {
    return String.format(counterNameFormat, getOperatorId(), ctrName);
  }

  public void initOperatorId() {
    setOperatorId(getName() + "_" + this.id);
  }

  public void setOperatorId(String operatorId) {
    this.operatorId = operatorId;
  }

  public HashMap<String, Long> getCounters() {
    return counters;
  }

  /**
   * called in ExecDriver.progress periodically.
   *
   * @param ctrs
   *          counters from the running job
   */
  @SuppressWarnings("unchecked")
  public void updateCounters(Counters ctrs) {
    if (counters == null) {
      counters = new HashMap<String, Long>();
    }

    // For some old unit tests, the counters will not be populated. Eventually,
    // the old tests should be removed
    if (counterNameToEnum == null) {
      return;
    }

    for (Map.Entry<String, ProgressCounter> counter : counterNameToEnum
        .entrySet()) {
      counters.put(counter.getKey(), ctrs.getCounter(counter.getValue()));
    }
    // update counters of child operators
    // this wont be an infinite loop since the operator graph is acyclic
    // but, some operators may be updated more than once and that's ok
    if (getChildren() != null) {
      for (Node op : getChildren()) {
        ((Operator<? extends OperatorDesc>) op).updateCounters(ctrs);
      }
    }
  }

  /**
   * Recursively check this operator and its descendants to see if the fatal
   * error counter is set to non-zero.
   *
   * @param ctrs
   */
  public boolean checkFatalErrors(Counters ctrs, StringBuilder errMsg) {
    if (counterNameToEnum == null) {
      return false;
    }

    String counterName = getWrappedCounterName(fatalErrorCntr);
    ProgressCounter pc = counterNameToEnum.get(counterName);

    // Currently, we maintain fixed number of counters per plan - in case of a
    // bigger tree, we may run out of them
    if (pc == null) {
      LOG
          .warn("Using too many counters. Increase the total number of counters for "
          + counterName);
    } else {
      long value = ctrs.getCounter(pc);
      fatalErrorMessage(errMsg, value);
      if (value != 0) {
        return true;
      }
    }

    if (getChildren() != null) {
      for (Node op : getChildren()) {
        if (((Operator<? extends OperatorDesc>) op).checkFatalErrors(ctrs,
            errMsg)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Get the fatal error message based on counter's code.
   *
   * @param errMsg
   *          error message should be appended to this output parameter.
   * @param counterValue
   *          input counter code.
   */
  protected void fatalErrorMessage(StringBuilder errMsg, long counterValue) {
  }

  // A given query can have multiple map-reduce jobs
  public static void resetLastEnumUsed() {
    lastEnumUsed = 0;
  }

  /**
   * Called only in SemanticAnalyzer after all operators have added their own
   * set of counter names.
   */
  public void assignCounterNameToEnum() {
    if (counterNameToEnum != null) {
      return;
    }
    counterNameToEnum = new HashMap<String, ProgressCounter>();
    for (String counterName : getCounterNames()) {
      ++lastEnumUsed;

      // TODO Hack for hadoop-0.17
      // Currently, only maximum number of 'totalNumCntrs' can be used. If you
      // want
      // to add more counters, increase the number of counters in
      // ProgressCounter
      if (lastEnumUsed > totalNumCntrs) {
        LOG
            .warn("Using too many counters. Increase the total number of counters");
        return;
      }
      String enumName = "C" + lastEnumUsed;
      ProgressCounter ctr = ProgressCounter.valueOf(enumName);
      counterNameToEnum.put(counterName, ctr);
    }
  }

  protected static String numInputRowsCntr = "NUM_INPUT_ROWS";
  protected static String numOutputRowsCntr = "NUM_OUTPUT_ROWS";
  protected static String timeTakenCntr = "TIME_TAKEN";
  protected static String fatalErrorCntr = "FATAL_ERROR";
  private static String counterNameFormat = "CNTR_NAME_%s_%s";

  public void initializeCounters() {
    initOperatorId();
    counterNames = new ArrayList<String>();
    counterNames.add(getWrappedCounterName(numInputRowsCntr));
    counterNames.add(getWrappedCounterName(numOutputRowsCntr));
    counterNames.add(getWrappedCounterName(timeTakenCntr));
    counterNames.add(getWrappedCounterName(fatalErrorCntr));
    /* getAdditionalCounter should return Wrapped counters */
    List<String> newCntrs = getAdditionalCounters();
    if (newCntrs != null) {
      counterNames.addAll(newCntrs);
    }
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

  public HashMap<String, ProgressCounter> getCounterNameToEnum() {
    return counterNameToEnum;
  }

  public void setCounterNameToEnum(
      HashMap<String, ProgressCounter> counterNameToEnum) {
    this.counterNameToEnum = counterNameToEnum;
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
    if(this.childOperators != null) {
      for (int i = 0; i<this.childOperators.size();i++) {
        Operator<? extends OperatorDesc> op = this.childOperators.get(i);
        op.setExecContext(execContext);
      }
    }
  }

  // The input file has changed - every operator can invoke specific action
  // for each input file
  public void cleanUpInputFileChanged() throws HiveException {
    this.cleanUpInputFileChangedOp();
    if(this.childOperators != null) {
      for (int i = 0; i<this.childOperators.size();i++) {
        Operator<? extends OperatorDesc> op = this.childOperators.get(i);
        op.cleanUpInputFileChanged();
      }
    }
  }

  // If a operator needs to invoke specific cleanup, that operator can override
  // this method
  public void cleanUpInputFileChangedOp() throws HiveException {
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
        parentClones.add((Operator<? extends OperatorDesc>)(parent.clone()));
      }
    }

    T descClone = (T)conf.clone();
    Operator<? extends OperatorDesc> ret =
      (Operator<? extends OperatorDesc>) OperatorFactory.getAndMakeChild(
        descClone, getSchema(), parentClones);

    return ret;
  }

  /**
   * Clones only the operator. The children and parent are set
   * to null.
   * @return Cloned operator
   * @throws CloneNotSupportedException
   */
  public Operator<? extends OperatorDesc> cloneOp() throws CloneNotSupportedException {
    T descClone = (T) conf.clone();
    Operator<? extends OperatorDesc> ret =
        (Operator<? extends OperatorDesc>) OperatorFactory.getAndMakeChild(
            descClone, getSchema());
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
    // Fix parent in all children
    if (this.getChildOperators() == null) {
      newOp.setChildOperators(null);
      return newOp;
    }
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
    return getName() + "[" + getIdentifier() + "]";
  }

  public static String toString(Collection<Operator<? extends OperatorDesc>> top) {
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
}
