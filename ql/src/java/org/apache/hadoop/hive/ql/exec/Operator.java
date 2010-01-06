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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.explain;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.OutputCollector;
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
  public static enum State {
    UNINIT,   // initialize() has not been called
    INIT,     // initialize() has been called and close() has not been called,
              // or close() has been called but one of its parent is not closed.
    CLOSE     // all its parents operators are in state CLOSE and called close()
              // to children. Note: close() being called and its state being CLOSE is
              // difference since close() could be called but state is not CLOSE if
              // one of its parent is not in state CLOSE..
  };
  transient protected State state = State.UNINIT;

  transient static boolean fatalError = false; // fatalError is shared acorss all operators
  
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
    return done || fatalError;
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
  transient protected ObjectInspector[] inputObjInspectors = new ObjectInspector[Short.MAX_VALUE];
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
      if ( reporter != null ) {
        childOperatorsArray[i].setReporter(reporter);
      }
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
    if ( fatalError ) 
      return;
    preProcessCounter();
    processOp(row, tag);
    postProcessCounter();
  }

  // If a operator wants to do some work at the beginning of a group
  public void startGroup() throws HiveException {
    LOG.debug("Starting group");

    if (childOperators == null)
      return;
    
    if ( fatalError )
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

    if ( fatalError )
      return;

    LOG.debug("Ending group for children:");
    for (Operator<? extends Serializable> op: childOperators)
      op.endGroup();

    LOG.debug("End group Done");
  }

  private boolean allInitializedParentsAreClosed() {
    if (parentOperators != null) {
      for(Operator<? extends Serializable> parent: parentOperators) {
        if (!(parent.state == State.CLOSE ||
              parent.state == State.UNINIT)) {
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

    if (state == State.CLOSE)
      return;

    // check if all parents are finished
    if (!allInitializedParentsAreClosed())
      return;

    // set state as CLOSE as long as all parents are closed
    // state == CLOSE doesn't mean all children are also in state CLOSE
    state = State.CLOSE;
    LOG.info(id + " finished. closing... ");

    if (counterNameToEnum != null) {
      incrCounter(numInputRowsCntr, inputRows);
      incrCounter(numOutputRowsCntr, outputRows);
      incrCounter(timeTakenCntr, totalTime);
    }

    LOG.info(id + " forwarded " + cntr + " rows");

    // call the operator specific close routine
    closeOp(abort);

    try {
      logStats();
      if(childOperators == null)
        return;

      for(Operator<? extends Serializable> op: childOperators) {
        op.close(abort);
      }

      LOG.info(id + " Close done");
    } catch (HiveException e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Operator specific close routine. Operators which inherents this
   * class should overwrite this funtion for their specific cleanup
   * routine.
   */
  protected void closeOp(boolean abort) throws HiveException {
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
    return dump(level, new HashSet<Integer>());
  }

  public String dump(int level, HashSet<Integer> seenOpts) {
    if ( seenOpts.contains(new Integer(id)))
      return null;
    seenOpts.add(new Integer(id));

    StringBuilder s = new StringBuilder();
    String ls = getLevelString(level);
    s.append(ls);
    s.append("<" + getName() + ">");
    s.append("Id =" + id);

    if (childOperators != null) {
      s.append(ls);
      s.append("  <Children>");
      for (Operator<? extends Serializable> o : childOperators) {
        s.append(o.dump(level+2, seenOpts));
      }
      s.append(ls);
      s.append("  <\\Children>");
    }

    if (parentOperators != null) {
      s.append(ls);
      s.append("  <Parent>");
      for (Operator<? extends Serializable> o : parentOperators) {
        s.append("Id = " + o.id + " ");
        s.append(o.dump(level,seenOpts));
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
    C391, C392, C393, C394, C395, C396, C397, C398, C399, C400
  };

  private static int totalNumCntrs = 400;

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

    if (counterNameToEnum != null) {
      if ((inputRows % 1000) == 0) {
        incrCounter(numInputRowsCntr, inputRows);
        incrCounter(timeTakenCntr, totalTime);
        inputRows = 0 ;
        totalTime = 0;
      }
      beginTime = System.currentTimeMillis();
    }
  }

  /**
   * this is called after operator process to buffer some counters
   */
  private void postProcessCounter()
  {
    if (counterNameToEnum != null)
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

    // Currently, we maintain fixed number of counters per plan - in case of a bigger tree, we may run out of them
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

  /**
   * Recursively check this operator and its descendants to see if the
   * fatal error counter is set to non-zero.
   * @param ctrs
   */
  public boolean checkFatalErrors(Counters ctrs, StringBuffer errMsg) {
    if ( counterNameToEnum == null )
      return false;
    
    String counterName = "CNTR_NAME_" + getOperatorId() + "_" + fatalErrorCntr;
    ProgressCounter pc = counterNameToEnum.get(counterName);

    // Currently, we maintain fixed number of counters per plan - in case of a bigger tree, we may run out of them
    if (pc == null)
      LOG.warn("Using too many counters. Increase the total number of counters for " + counterName);
    else {
      long value = ctrs.getCounter(pc);
      fatalErrorMessage(errMsg, value);
      if ( value != 0 ) 
        return true;
    }
    
    if (getChildren() != null) {
      for (Node op: getChildren()) {
        if (((Operator<? extends Serializable>)op).checkFatalErrors(ctrs, errMsg)) {
          return true;
        }
      }
    }
    return false;
  }
  
  /** 
   * Get the fatal error message based on counter's code.
   * @param errMsg error message should be appended to this output parameter.
   * @param counterValue input counter code.
   */
  protected void fatalErrorMessage(StringBuffer errMsg, long counterValue) {
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
  protected static String fatalErrorCntr    = "FATAL_ERROR";

  public void initializeCounters() {
    initOperatorId();
    counterNames = new ArrayList<String>();
    counterNames.add("CNTR_NAME_" + getOperatorId() + "_" + numInputRowsCntr);
    counterNames.add("CNTR_NAME_" + getOperatorId() + "_" + numOutputRowsCntr);
    counterNames.add("CNTR_NAME_" + getOperatorId() + "_" + timeTakenCntr);
    counterNames.add("CNTR_NAME_" + getOperatorId() + "_" + fatalErrorCntr);
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

   /**
   * Should be overridden to return the type of the specific operator among
    * the types in OperatorType
    *
    * @return OperatorType.* or -1 if not overridden
    */
   public int getType() {
     assert false;
     return -1;
   }
}
