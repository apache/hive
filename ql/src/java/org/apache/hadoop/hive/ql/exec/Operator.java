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
import org.apache.hadoop.hive.ql.plan.OpTraits;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Base operator implementation.
 **/
public abstract class Operator<T extends OperatorDesc> implements Serializable,Cloneable,
  Node {

  // Bean methods

  private static final long serialVersionUID = 1L;

  public static final String HIVECOUNTERCREATEDFILES = "CREATED_FILES";
  public static final String HIVECOUNTERFATAL = "FATAL_ERROR";
  public static final String CONTEXT_NAME_KEY = "__hive.context.name";

  private transient Configuration configuration;
  protected List<Operator<? extends OperatorDesc>> childOperators;
  protected List<Operator<? extends OperatorDesc>> parentOperators;
  protected String operatorId;
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
  }

  protected transient State state = State.UNINIT;

  static {
    seqId = new AtomicInteger(0);
  }

  private boolean useBucketizedHiveInputFormat;

  // dummy operator (for not increasing seqId)
  private Operator(String name) {
    id = name;
  }

  public Operator() {
    id = String.valueOf(seqId.getAndIncrement());
    childOperators = new ArrayList<Operator<? extends OperatorDesc>>();
    parentOperators = new ArrayList<Operator<? extends OperatorDesc>>();
    initOperatorId();
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
  protected transient final Log LOG = LogFactory.getLog(getClass().getName());
  protected transient final Log PLOG = LogFactory.getLog(Operator.class.getName()); // for simple disabling logs from all operators
  protected transient final boolean isLogInfoEnabled = LOG.isInfoEnabled() && PLOG.isInfoEnabled();
  protected transient final boolean isLogDebugEnabled = LOG.isDebugEnabled() && PLOG.isDebugEnabled();
  protected transient final boolean isLogTraceEnabled = LOG.isTraceEnabled() && PLOG.isTraceEnabled();
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

  @SuppressWarnings("rawtypes")
  public void setOutputCollector(OutputCollector out) {
    this.out = out;

    // the collector is same across all operators
    if (childOperators == null) {
      return;
    }

    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.setOutputCollector(out);
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
  @SuppressWarnings("unchecked")
  public void initialize(Configuration hconf, ObjectInspector[] inputOIs)
      throws HiveException {
    if (state == State.INIT) {
      return;
    }

    this.configuration = hconf;
    if (!areAllParentsInitialized()) {
      return;
    }

    LOG.info("Initializing Self " + this);

    if (inputOIs != null) {
      inputObjInspectors = inputOIs;
    }

    // initialize structure to maintain child op info. operator tree changes
    // while
    // initializing so this need to be done here instead of initialize() method
    if (childOperators != null && !childOperators.isEmpty()) {
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

    // sanity check
    if (childOperatorsArray == null
        && !(childOperators == null || childOperators.isEmpty())) {
      throw new HiveException(
          "Internal Hive error during operator initialization.");
    }

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
    if (childOperators == null || childOperators.isEmpty()) {
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
  public abstract void processOp(Object row, int tag) throws HiveException;

  protected final void defaultStartGroup() throws HiveException {
    if (isLogDebugEnabled) {
      LOG.debug("Starting group");
    }

    if (childOperators == null) {
      return;
    }

    if (isLogDebugEnabled) {
      LOG.debug("Starting group for children:");
    }
    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.startGroup();
    }

    if (isLogDebugEnabled) {
      LOG.debug("Start group Done");
    }
  }

  protected final void defaultEndGroup() throws HiveException {
    if (isLogDebugEnabled) {
      LOG.debug("Ending group");
    }

    if (childOperators == null) {
      return;
    }

    if (isLogDebugEnabled) {
      LOG.debug("Ending group for children:");
    }
    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.endGroup();
    }

    if (isLogDebugEnabled) {
      LOG.debug("End group Done");
    }
  }

  // If a operator wants to do some work at the beginning of a group
  public void startGroup() throws HiveException {
    defaultStartGroup();
  }

  // If an operator wants to do some work at the end of a group
  public void endGroup() throws HiveException {
    defaultEndGroup();
  }

  // an blocking operator (e.g. GroupByOperator and JoinOperator) can
  // override this method to forward its outputs
  public void flush() throws HiveException {
  }

  public void processGroup(int tag) throws HiveException {
    if (childOperators == null || childOperators.isEmpty()) {
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
        LOG.debug("allInitializedParentsAreClosed? parent.state = " + parent.state);
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
      LOG.debug("Not all parent operators are closed. Not closing.");
      return;
    }

    // set state as CLOSE as long as all parents are closed
    // state == CLOSE doesn't mean all children are also in state CLOSE
    state = State.CLOSE;
    LOG.info(id + " finished. closing... ");

    // call the operator specific close routine
    closeOp(abort);

    reporter = null;

    try {
      logStats();
      if (childOperators == null) {
        return;
      }

      for (Operator<? extends OperatorDesc> op : childOperators) {
        LOG.debug("Closing child = " + op);
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

    if (childOperators != null) {
      for (Operator<? extends OperatorDesc> op : childOperators) {
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

  // Remove the operators till a certain depth.
  // Return true if the remove was successful, false otherwise
  public boolean removeChildren(int depth) {
    Operator<? extends OperatorDesc> currOp = this;
    for (int i = 0; i < depth; i++) {
      // If there are more than 1 children at any level, don't do anything
      if ((currOp.getChildOperators() == null) || (currOp.getChildOperators().isEmpty()) ||
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

    if ((childOperatorsArray == null) || (getDone())) {
      return;
    }

    int childrenDone = 0;
    for (int i = 0; i < childOperatorsArray.length; i++) {
      Operator<? extends OperatorDesc> o = childOperatorsArray[i];
      if (o.getDone()) {
        childrenDone++;
      } else {
        o.processOp(row, childOperatorsTag[i]);
      }
    }

    // if all children are done, this operator is also done
    if (childrenDone == childOperatorsArray.length) {
      setDone(true);
    }
  }

  public void resetStats() {
    for (String e : statsMap.keySet()) {
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
    for (String e : statsMap.keySet()) {
      LOG.info(e.toString() + ":" + statsMap.get(e).toString());
    }
  }

  /**
   * Implements the getName function for the Node Interface.
   *
   * @return the name of the operator
   */
  @Override
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

  protected transient Object groupKeyObject;

  public String getOperatorId() {
    return operatorId;
  }

  public void initOperatorId() {
    setOperatorId(getName() + "_" + this.id);
  }

  public void setOperatorId(String operatorId) {
    this.operatorId = operatorId;
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

  // called by map operator. propagated recursively to single parented descendants
  public void setInputContext(String inputPath, String tableName, String partitionName) {
    if (childOperators != null) {
      for (Operator<? extends OperatorDesc> child : childOperators) {
        if (child.getNumParent() == 1) {
          child.setInputContext(inputPath, tableName, partitionName);
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
    Operator<? extends OperatorDesc> ret =
        OperatorFactory.getAndMakeChild(descClone, getSchema(), getColumnExprMap(), parentClones);

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
        OperatorFactory.getAndMakeChild(
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

  public Statistics getStatistics() {
    if (conf != null) {
      return conf.getStatistics();
    }
    return null;
  }

  public OpTraits getOpTraits() {
    if (conf != null) {
      return conf.getOpTraits();
    }

    return null;
  }

  public void setOpTraits(OpTraits metaInfo) {
    if (isLogDebugEnabled) {
      LOG.debug("Setting traits ("+metaInfo+") on "+this);
    }
    if (conf != null) {
      conf.setOpTraits(metaInfo);
    } else {
      LOG.warn("Cannot set traits when there's no descriptor: "+this);
    }
  }

  public void setStatistics(Statistics stats) {
    if (isLogDebugEnabled) {
      LOG.debug("Setting stats ("+stats+") on "+this);
    }
    if (conf != null) {
      conf.setStatistics(stats);
    } else {
      LOG.warn("Cannot set stats when there's no descriptor: "+this);
    }
  }

  public static Operator createDummy() {
    return new DummyOperator();
  }

  private static class DummyOperator extends Operator {
    public DummyOperator() { super("dummy"); }
    @Override
    public void processOp(Object row, int tag) { }
    @Override
    public OperatorType getType() { return null; }
  }

  public Map<Integer, DummyStoreOperator> getTagToOperatorTree() {
    if ((parentOperators == null) || (parentOperators.size() == 0)) {
      return null;
    }
    Map<Integer, DummyStoreOperator> dummyOps = parentOperators.get(0).getTagToOperatorTree();
    return dummyOps;
  }
}
