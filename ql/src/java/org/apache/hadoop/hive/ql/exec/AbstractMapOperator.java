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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;


/**
 * Abstract Map operator. Common code of MapOperator and VectorMapOperator.
 **/
@SuppressWarnings("deprecation")
public abstract class AbstractMapOperator extends Operator<MapWork>
    implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;

  /**
   * Initialization call sequence:
   *
   *   (Operator)                     Operator.setConf(MapWork conf);
   *   (Operator)                     Operator.initialize(
   *                                      Configuration hconf, ObjectInspector[] inputOIs);
   *
   *   ([Vector]MapOperator)          @Override setChildren(Configuration hconf)
   *
   *   (Operator)                     Operator.passExecContext(ExecMapperContext execContext)
   *   (Operator)                     Operator.initializeLocalWork(Configuration hconf)
   *
   *   (AbstractMapOperator)          initializeMapOperator(Configuration hconf)
   *
   * [ (AbstractMapOperator)          initializeContexts() ]   // exec.tez.MapRecordProcessor only.
   *
   *   (Operator)                     Operator.setReporter(Reporter rep)
   *
   */

  /**
   * Counter.
   *
   */
  public static enum Counter {
    DESERIALIZE_ERRORS,
    RECORDS_IN
  }

  protected final transient LongWritable deserialize_error_count = new LongWritable();
  protected final transient LongWritable recordCounter = new LongWritable();
  protected transient long numRows = 0;

  private final Map<Integer, DummyStoreOperator> connectedOperators
  = new TreeMap<Integer, DummyStoreOperator>();

  private transient final Map<Path, Path> normalizedPaths = new HashMap<>();

  private Path normalizePath(Path onefile, boolean schemaless) {
    //creating Path is expensive, so cache the corresponding
    //Path object in normalizedPaths
    Path path = normalizedPaths.get(onefile);
    if (path == null) {
      path = onefile;
      if (schemaless && path.toUri().getScheme() != null) {
        path = new Path(path.toUri().getPath());
      }
      normalizedPaths.put(onefile, path);
    }
    return path;
  }

  protected String getNominalPath(Path fpath) {
    Path nominal = null;
    boolean schemaless = fpath.toUri().getScheme() == null;
    for (Path onefile : conf.getPathToAliases().keySet()) {
      Path onepath = normalizePath(onefile, schemaless);
      Path curfpath = fpath;
      if(!schemaless && onepath.toUri().getScheme() == null) {
        curfpath = new Path(fpath.toUri().getPath());
      }
      // check for the operators who will process rows coming to this Map Operator
      if (onepath.toUri().relativize(curfpath.toUri()).equals(curfpath.toUri())) {
        // not from this
        continue;
      }
      if (nominal != null) {
        throw new IllegalStateException("Ambiguous input path " + fpath);
      }
      nominal = onefile;
      break;
    }
    if (nominal == null) {
      throw new IllegalStateException("Invalid input path " + fpath);
    }
    return nominal.toString();
  }

  public abstract void initEmptyInputChildren(List<Operator<?>> children, Configuration hconf)
      throws SerDeException, Exception;


  /** Kryo ctor. */
  protected AbstractMapOperator() {
    super();
  }

  public AbstractMapOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public abstract void setChildren(Configuration hconf) throws Exception;


  public void initializeMapOperator(Configuration hconf) throws HiveException {
    // set that parent initialization is done and call initialize on children
    state = State.INIT;

    statsMap.put(Counter.DESERIALIZE_ERRORS.toString(), deserialize_error_count);

    numRows = 0;

    String context = hconf.get(Operator.CONTEXT_NAME_KEY, "");
    if (context != null && !context.isEmpty()) {
      context = "_" + context.replace(" ","_");
    }
    statsMap.put(Counter.RECORDS_IN + context, recordCounter);
  }

  public abstract void initializeContexts() throws HiveException;

  public abstract Deserializer getCurrentDeserializer();

  public abstract void process(Writable value) throws HiveException;

  @Override
  public void closeOp(boolean abort) throws HiveException {
    recordCounter.set(numRows);
    super.closeOp(abort);
  }

  public void clearConnectedOperators() {
    connectedOperators.clear();
  }

  public void setConnectedOperators(int tag, DummyStoreOperator dummyOp) {
    connectedOperators.put(tag, dummyOp);
  }

  public Map<Integer, DummyStoreOperator> getConnectedOperators() {
    return connectedOperators;
  }
}