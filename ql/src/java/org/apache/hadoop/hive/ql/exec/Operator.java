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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.explain;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;

/**
 * Base operator implementation
 **/
public abstract class Operator <T extends Serializable> implements Serializable, Node {

  // Bean methods

  private static final long serialVersionUID = 1L;
  
  protected List<Operator<? extends Serializable>> childOperators;
  protected List<Operator<? extends Serializable>> parentOperators;
  protected String operatorId;
  /**
   * List of counter names associated with the operator
   * It contains the following default counters
   *   NUM_INPUT_ROWS
   *   NUM_OUTPUT_ROWS
   *   TIME_TAKEN
   * Individual operators can add to this list via addToCounterNames methods
   */
  protected ArrayList<String> counterNames;
  
  /**
   * Each operator has its own map of its counter names to disjoint
   * ProgressCounter - it is populated at compile time and is read in
   * at run-time while extracting the operator specific counts
   */
  protected HashMap<String, ProgressCounter> counterNameToEnum;
  

  private static int seqId;

  // It can be optimized later so that an operator operator (init/close) is performed
  // only after that operation has been performed on all the parents. This will require
  // initializing the whole tree in all the mappers (which might be required for mappers
  // spanning multiple files anyway, in future)
  public static enum State { UNINIT, INIT, CLOSE };
  transient protected State state = State.UNINIT;

  static {
    seqId = 0;
  }
  
  public Operator() {
    id = String.valueOf(seqId++);
  }
  
  /**
   * Create an operator with a reporter.
   * @param reporter Used to report progress of certain operators.
   */
  public Operator(Reporter reporter) {
    this.reporter = reporter;
    id = String.valueOf(seqId++);
  }

  public void setChildOperators(List<Operator<? extends Serializable>> childOperators) {
    this.childOperators = childOperators;
  }

  public List<Operator<? extends Serializable>> getChildOperators() {
    return childOperators;
  }

  /**
   * Implements the getChildren function for the Node Interface.
   */
  public Vector<Node> getChildren() {
    
    if (getChildOperators() == null) {
      return null;
    }
    
    Vector<Node> ret_vec = new Vector<Node>();
    for(Operator<? extends Serializable> op: getChildOperators()) {
      ret_vec.add(op);
    }
    
    return ret_vec;
  }
  
  public void setParentOperators(List<Operator<? extends Serializable>> parentOperators) {
    this.parentOperators = parentOperators;
  }

  public List<Operator<? extends Serializable>> getParentOperators() {
    return parentOperators;
  }

  protected T conf;
  protected boolean done;

  public void setConf(T conf) {
    this.conf = conf;
  }

  @explain
  public T getConf() {
    return conf;
  }

  public boolean getDone() {
    return done;
  }

  public void setDone(boolean done) {
    this.done = done;
  }

  // non-bean fields needed during compilation
  transient private RowSchema rowSchema;

  public void setSchema(RowSchema rowSchema) {
    this.rowSchema = rowSchema;
  }

  public RowSchema getSchema() {
    return rowSchema;
  }
  
  // non-bean ..

  transient protected HashMap<Enum<?>, LongWritable> statsMap = new HashMap<Enum<?>, LongWritable> ();
  transient protected OutputCollector out;
  transient protected Log LOG = LogFactory.getLog(this.getClass().getName());
  transient protected String alias;
  transient protected Reporter reporter;
  transient protected String id;
  // object inspectors for input rows
  transient protected ObjectInspector[] inputObjInspectors = new ObjectInspector[Byte.MAX_VALUE];
  // for output rows of this operator
  transient protected ObjectInspector outputObjInspector; 

  /**
   * A map of output column name to input expression map. This is used by optimizer
   * and built during semantic analysis
   * contains only key elements for reduce sink and group by op
   */
  protected transient Map<String, exprNodeDesc> colExprMap;

  public void setId(String id) {
    this.id = id;
  }
  
  /**
   * This function is not named getId(), to make sure java serialization
   * does NOT serialize it.  Some TestParse tests will fail if we serialize
   * this field, since the Operator ID will change based on the number of
   * query tests.
   */
  public String getIdentifier() { return id; }
  
  public void setReporter(Reporter rep) {
    reporter = rep;

    // the collector is same across all operators
    if(childOperators == null)
      return;

    for(Operator<? extends Serializable> op: childOperators) {
      op.setReporter(rep);
    }
  }
  
  public void setOutputCollector(OutputCollector out) {
    this.out = out;

    // the collector is same across all operators
    if(childOperators == null)
      return;

    for(Operator<? extends Serializable> op: childOperators) {
      op.setOutputCollector(out);
    }
  }

  /**
   * Store the alias this operator is working on behalf of
   */
  public void setAlias(String alias) {
    this.alias = alias;

    if(childOperators == null)
      return;

    for(Operator<? extends Serializable> op: childOperators) {
      op.setAlias(alias);
    }
  }

  public Map<Enum<?>, Long> getStats() {
    HashMap<Enum<?>, Long> ret = new HashMap<Enum<?>, Long> ();
    for(Enum<?> one: statsMap.keySet()) {
      ret.put(one, Long.valueOf(statsMap.get(one).get()));
    }
    return(ret);
  }

  /**
   * checks whether all parent operators are initialized or not
   * @return true if there are no parents or all parents are initialized. false otherwise
   */
  protected boolean areAllParentsInitialized() {
    if (parentOperators == null) {
      return true;
    }
    for(Operator<? extends Serializable> parent: parentOperators) {
      if (parent.state != State.INIT) {
        return false;
      }
    }
    return true;
  }

  /**
   * Initializes operators only if all parents have been initialized.
   * Calls operator specific initializer which then initializes child ops.
   * 
   * @param hconf
   * @param inputOIs input object inspector array indexes by tag id. null value is ignored.
   * @throws HiveException
   */
  public void initialize(Configuration hconf, ObjectInspector[] inputOIs) throws HiveException {
    if (state == State.INIT) {
      return;
    }

    if(!areAllParentsInitialized()) {
      return;
    }
    LOG.info("Initializing Self " + id + " " + getName());
    
    if (inputOIs != null) {
      inputObjInspectors = inputOIs;
    }
    
    // initialize structure to maintain child op info. operator tree changes while
    // initializing so this need to be done here instead of initialize() method
    if (childOperators != null) {
      childOperatorsArray = new Operator[childOperators.size()];
      for (int i=0; i<childOperatorsArray.length; i++) {
        childOperatorsArray[i] = childOperators.get(i); 
      }
      childOperatorsTag = new int[childOperatorsArray.length];
      for (int i=0; i<childOperatorsArray.length; i++) {
        List<Operator<? extends Serializable>> parentOperators = 
          childOperatorsArray[i].getParentOperators();
        if (parentOperators == null) {
          throw new HiveException("Hive internal error: parent is null in " 
              + childOperatorsArray[i].getClass() + "!");
        }
        childOperatorsTag[i] = parentOperators.indexOf(this);
        if (childOperatorsTag[i] == -1) {
          throw new HiveException("Hive internal error: cannot find parent in the child operator!");
        }
      }
    }
    
    if (inputObjInspectors.length == 0) {
      throw new HiveException("Internal Error during operator initialization.");
    }
    // derived classes can set this to different object if needed
    outputObjInspector = inputObjInspectors[0];
    initializeOp(hconf);
    LOG.info("Initialization Done " + id + " " + getName());
  }

  /**
   * Operator specific initialization.
   */
  protected void initializeOp(Configuration hconf) throws HiveException {
    initializeChildren(hconf);
  }
 
  /**
   * Calls initialize on each of the children with outputObjetInspector as the output row format
   */
  protected void initializeChildren(Configuration hconf) throws HiveException {
    state = State.INIT;
    LOG.info("Operator " + id + " " + getName() + " initialized");
    if (childOperators == null) {
      return;
    }
    LOG.info("Initializing children of " + id + " " + getName());
    for (int i = 0; i < childOperatorsArray.length; i++) {
      childOperatorsArray[i].initialize(hconf, outputObjInspector, childOperatorsTag[i]);
    }
  }

  /**
   * Collects all the parent's output object inspectors and calls actual initialization method
   * @param hconf
   * @param inputOI OI of the row that this parent will pass to this op
   * @param parentId parent operator id
   * @throws HiveException
   */
  private void initialize(Configuration hconf, ObjectInspector inputOI, int parentId) throws HiveException {
    LOG.info("Initializing child " + id + " " + getName());
    inputObjInspectors[parentId] = inputOI;
    // call the actual operator initialization function
    initialize(hconf, null);    
  }

  
   /**
   * Process the row.
   * @param row  The object representing the row.
   * @param tag  The tag of the row usually means which parent this row comes from.
   *             Rows with the same tag should have exactly the same rowInspector all the time.
   */
  public abstract void processOp(Object row, int tag) throws HiveException;
  
  /**
   * Process the row.
   * @param row  The object representing the row.
   * @param tag  The tag of the row usually means which parent this row comes from.
   *             Rows with the same tag should have exactly the same rowInspector all the time.
   */
  public void process(Object row, int tag) throws HiveException {
    preProcessCounter();
    processOp(row, tag);
    postProcessCounter();
  }
  
  // If a operator wants to do some work at the beginning of a group
  public void startGroup() throws HiveException {
    LOG.debug("Starting group");
    
    if (childOperators == null)
      return;
    
    LOG.debug("Starting group for children:");
    for (Operator<? extends Serializable> op: childOperators)
      op.startGroup();
    
    LOG.debug("Start group Done");
  }  
  
  // If a operator wants to do some work at the end of a group
  public void endGroup() throws HiveException {
    LOG.debug("Ending group");
    
    if (childOperators == null)
      return;
    
    LOG.debug("Ending group for children:");
    for (Operator<? extends Serializable> op: childOperators)
      op.endGroup();
    
    LOG.debug("End group Done");
  }

  public void close(boolean abort) throws HiveException {
    if (state == State.CLOSE) 
      return;

    if (counterNameToEnum != null) {
      incrCounter(numInputRowsCntr, inputRows);
      incrCounter(numOutputRowsCntr, outputRows);
      incrCounter(timeTakenCntr, totalTime);
    }
    
    LOG.info(id + " forwarded " + cntr + " rows");

    try {
      logStats();
      if(childOperators == null)
        return;

      for(Operator<? extends Serializable> op: childOperators) {
        op.close(abort);
      }
      state = State.CLOSE;

      LOG.info("Close done");
    } catch (HiveException e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Unlike other operator interfaces which are called from map or reduce task,
   * jobClose is called from the jobclient side once the job has completed
   *
   * @param conf Configuration with with which job was submitted
   * @param success whether the job was completed successfully or not
   */
  public void jobClose(Configuration conf, boolean success) throws HiveException {
    if(childOperators == null)
      return;
    
    for(Operator<? extends Serializable> op: childOperators) {
      op.jobClose(conf, success);
    }
  }

  /**
   *  Cache childOperators in an array for faster access. childOperatorsArray is accessed
   *  per row, so it's important to make the access efficient.
   */
  transient protected Operator<? extends Serializable>[] childOperatorsArray = null;
  transient protected int[] childOperatorsTag; 

  // counters for debugging
  transient private long cntr = 0;
  transient private long nextCntr = 1;

   /**
   * Replace one child with another at the same position. The parent of the child is not changed
   * @param child     the old child
   * @param newChild  the new child
   */
  public void  replaceChild(Operator<? extends Serializable> child, Operator<? extends Serializable> newChild) {
    int childIndex = childOperators.indexOf(child);
    assert childIndex != -1;
    childOperators.set(childIndex, newChild);
  }

  public void  removeChild(Operator<? extends Serializable> child) {
    int childIndex = childOperators.indexOf(child);
    assert childIndex != -1;
    if (childOperators.size() == 1) 
      childOperators = null;
    else
      childOperators.remove(childIndex);
    
    int parentIndex = child.getParentOperators().indexOf(this);
    assert parentIndex != -1;
    if (child.getParentOperators().size() == 1)
      child.setParentOperators(null);
    else
      child.getParentOperators().remove(parentIndex);
  }

  /**
   * Replace one parent with another at the same position. Chilren of the new parent are not updated
   * @param parent     the old parent
   * @param newParent  the new parent
   */
  public void  replaceParent(Operator<? extends Serializable> parent, Operator<? extends Serializable> newParent) {
    int parentIndex = parentOperators.indexOf(parent);
    assert parentIndex != -1;
    parentOperators.set(parentIndex, newParent);
  }

  private long getNextCntr(long cntr) {
    // A very simple counter to keep track of number of rows processed by an operator. It dumps
    // every 1 million times, and quickly before that
    if (cntr >= 1000000)
      return cntr + 1000000;
    
    return 10 * cntr;
  }

  protected void forward(Object row, ObjectInspector rowInspector) throws HiveException {
    
    if ((++outputRows % 1000) == 0) {
      if (counterNameToEnum != null) {
        incrCounter(numOutputRowsCntr, outputRows);
        outputRows = 0;
      }
    }
    
    if (LOG.isInfoEnabled()) {
      cntr++;
      if (cntr == nextCntr) {
        LOG.info(id + " forwarding " + cntr + " rows");
        nextCntr = getNextCntr(cntr);
      }
    }

    // For debugging purposes:
    // System.out.println("" + this.getClass() + ": " + SerDeUtils.getJSONString(row, rowInspector));
    // System.out.println("" + this.getClass() + ">> " + ObjectInspectorUtils.getObjectInspectorName(rowInspector));
 
    if (childOperatorsArray == null && childOperators != null) {
      throw new HiveException("Internal Hive error during operator initialization.");
    }
    
    if((childOperatorsArray == null) || (getDone())) {
      return;
    }
    
    int childrenDone = 0;
    for (int i = 0; i < childOperatorsArray.length; i++) {
      Operator<? extends Serializable> o = childOperatorsArray[i];
      if (o.getDone()) {
        childrenDone ++;
      } else {
        o.process(row, childOperatorsTag[i]);
      }
    }
    
    // if all children are done, this operator is also done
    if (childrenDone == childOperatorsArray.length) {
      setDone(true);
    }
  }

  public void resetStats() {
    for(Enum<?> e: statsMap.keySet()) {
      statsMap.get(e).set(0L);
    }
  }

  public static interface OperatorFunc {
    public void func(Operator<? extends Serializable> op);
  }

  public void preorderMap (OperatorFunc opFunc) {
    opFunc.func(this);
    if(childOperators != null) {
      for(Operator<? extends Serializable> o: childOperators) {
        o.preorderMap(opFunc);
      }
    }
  }

  public void logStats () {
    for(Enum<?> e: statsMap.keySet()) {
      LOG.info(e.toString() + ":" + statsMap.get(e).toString());
    }    
  }

  /**
   * Implements the getName function for the Node Interface.
   * @return the name of the operator
   */
  public String getName() {
    return new String("OP");
  }

  /**
   * Returns a map of output column name to input expression map
   * Note that currently it returns only key columns for ReduceSink and GroupBy operators
   * @return null if the operator doesn't change columns
   */
  public Map<String, exprNodeDesc> getColumnExprMap() {
    return colExprMap;
  }

  public void setColumnExprMap(Map<String, exprNodeDesc> colExprMap) {
    this.colExprMap = colExprMap;
  }
  
  private String getLevelString(int level) {
    if (level == 0) {
      return "\n";
    }
    StringBuilder s = new StringBuilder();
    s.append("\n");
    while(level > 0) {
      s.append("  ");
      level--;
    }
    return s.toString();
  }
  
  public String dump(int level) {
    StringBuilder s = new StringBuilder();
    String ls = getLevelString(level);
    s.append(ls);
    s.append("<" + getName() + ">");
    s.append("Id =" + id);
    if (childOperators != null) {
      s.append(ls);
      s.append("  <Children>");
      for (Operator<? extends Serializable> o : childOperators) {
        s.append(o.dump(level+2));
      }
      s.append(ls);
      s.append("  <\\Children>");
    }

    if (parentOperators != null) {
      s.append(ls);
      s.append("  <Parent>");
      for (Operator<? extends Serializable> o : parentOperators) {
        s.append("Id = " + o.id + " ");
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
    for (int i=0; i<evals.length; i++) {
      result[i] = evals[i].initialize(rowInspector);
    }
    return result;
  }

  /**
   * Initialize an array of ExprNodeEvaluator and put the return values into a 
   * StructObjectInspector with integer field names.
   */  
  protected static StructObjectInspector initEvaluatorsAndReturnStruct(
      ExprNodeEvaluator[] evals, List<String> outputColName, ObjectInspector rowInspector) 
      throws HiveException {
    ObjectInspector[] fieldObjectInspectors = initEvaluators(evals, rowInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        outputColName,
        Arrays.asList(fieldObjectInspectors));
  }
  
  /**
   * All counter stuff below this
   */
  
  /**
   * TODO This is a hack for hadoop 0.17 which only supports enum counters
   */
  public static enum ProgressCounter { 
    C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15, C16, 
    C17, C18, C19, C20, C21, C22, C23, C24, C25, C26, C27, C28, C29, C30, C31, C32, 
    C33, C34, C35, C36, C37, C38, C39, C40, C41, C42, C43, C44, C45, C46, C47, C48, 
    C49, C50, C51, C52, C53, C54, C55, C56, C57, C58, C59, C60, C61, C62, C63, C64, 
    C65, C66, C67, C68, C69, C70, C71, C72, C73, C74, C75, C76, C77, C78, C79, C80, 
    C81, C82, C83, C84, C85, C86, C87, C88, C89, C90, C91, C92, C93, C94, C95, C96, 
    C97, C98, C99, C100, C101, C102, C103, C104, C105, C106, C107, C108, C109, C110, C111, C112, 
    C113, C114, C115, C116, C117, C118, C119, C120, C121, C122, C123, C124, C125, C126, C127, C128
  };

  private static int totalNumCntrs = 128;
  
  /**
   * populated at runtime from hadoop counters at run time in the client
   */
  transient protected Map<String, Long> counters;
  
  /**
   * keeps track of unique ProgressCounter enums used
   * this value is used at compile time while assigning ProgressCounter
   * enums to counter names
   */
  private static int lastEnumUsed;  

  transient protected long inputRows = 0;
  transient protected long outputRows = 0;
  transient protected long beginTime = 0;
  transient protected long totalTime = 0;
  
  /**
   * this is called before operator process to buffer some counters
   */
  private void preProcessCounter()
  {
    inputRows++;
    
    if (((inputRows % 1000) == 0) && (counterNameToEnum != null)) {
      incrCounter(numInputRowsCntr, inputRows);
      incrCounter(timeTakenCntr, totalTime);
      inputRows = 0 ;
      totalTime = 0;
    }
    beginTime = System.currentTimeMillis();
  }

  /**
   * this is called after operator process to buffer some counters
   */
  private void postProcessCounter()
  {
    totalTime += (System.currentTimeMillis() - beginTime);
  }

  
  /**
   * this is called in operators in map or reduce tasks
   * @param name
   * @param amount
   */
  protected void incrCounter(String name, long amount)
  {
    String counterName = "CNTR_NAME_" + getOperatorId() + "_" + name;
    ProgressCounter pc = counterNameToEnum.get(counterName);

    // Currently, we maintain 128 counters per plan - in case of a bigger tree, we may run out of them
    if (pc == null)
      LOG.warn("Using too many counters. Increase the total number of counters for " + counterName);
    else if (reporter != null)
      reporter.incrCounter(pc, amount);
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

  public void initOperatorId() {
    setOperatorId(getName() + "_" + this.id);
  }

  public void setOperatorId(String operatorId) {
    this.operatorId = operatorId;
  }
  
  public Map<String, Long> getCounters() {
    return counters;
  }
  
  /**
   * called in ExecDriver.progress periodically
   * @param ctrs counters from the running job
   */
  @SuppressWarnings("unchecked")
  public void updateCounters(Counters ctrs) {
    if (counters == null) {
      counters = new HashMap<String, Long>();
    }

    // For some old unit tests, the counters will not be populated. Eventually, the old tests should be removed
    if (counterNameToEnum == null)
      return;

    for (Map.Entry<String, ProgressCounter> counter: counterNameToEnum.entrySet()) {
      counters.put(counter.getKey(), ctrs.getCounter(counter.getValue()));
    }
    // update counters of child operators
    // this wont be an infinite loop since the operator graph is acyclic
    // but, some operators may be updated more than once and that's ok
    if (getChildren() != null) {
      for (Node op: getChildren()) {
        ((Operator<? extends Serializable>)op).updateCounters(ctrs);
      }
    }
  }

  // A given query can have multiple map-reduce jobs
  public static void resetLastEnumUsed() {
    lastEnumUsed = 0;
  }

  /**
   * Called only in SemanticAnalyzer after all operators have added their
   * own set of counter names
   */
  public void assignCounterNameToEnum() {
    if (counterNameToEnum != null) {
      return;
    }
    counterNameToEnum = new HashMap<String, ProgressCounter>();
    for (String counterName: getCounterNames()) {
      ++lastEnumUsed;
      
      // TODO Hack for hadoop-0.17
      // Currently, only maximum number of 'totalNumCntrs' can be used. If you want
      // to add more counters, increase the number of counters in ProgressCounter
      if (lastEnumUsed > totalNumCntrs) {
        LOG.warn("Using too many counters. Increase the total number of counters");
        return;
      }      
      String enumName = "C" + lastEnumUsed;
      ProgressCounter ctr = ProgressCounter.valueOf(enumName);
      counterNameToEnum.put(counterName, ctr);
    }
  }

  protected static String numInputRowsCntr  = "NUM_INPUT_ROWS";
  protected static String numOutputRowsCntr = "NUM_OUTPUT_ROWS";
  protected static String timeTakenCntr     = "TIME_TAKEN";
  
  public void initializeCounters() {
    initOperatorId();
    counterNames = new ArrayList<String>();
    counterNames.add("CNTR_NAME_" + getOperatorId() + "_" + numInputRowsCntr);
    counterNames.add("CNTR_NAME_" + getOperatorId() + "_" + numOutputRowsCntr);
    counterNames.add("CNTR_NAME_" + getOperatorId() + "_" + timeTakenCntr);
    List<String> newCntrs = getAdditionalCounters();
    if (newCntrs != null) {
      counterNames.addAll(newCntrs);
    }
  }

  /*
   * By default, the list is empty - if an operator wants to add more counters, it should override this method
   * and provide the new list.
   */
  private List<String> getAdditionalCounters() {
    return null;
  }

  public HashMap<String, ProgressCounter> getCounterNameToEnum() {
    return counterNameToEnum;
  }

  public void setCounterNameToEnum(HashMap<String, ProgressCounter> counterNameToEnum) {
    this.counterNameToEnum = counterNameToEnum;
  }


}
