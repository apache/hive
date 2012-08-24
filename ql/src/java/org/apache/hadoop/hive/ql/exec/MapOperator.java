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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

/**
 * Map operator. This triggers overall map side processing. This is a little
 * different from regular operators in that it starts off by processing a
 * Writable data structure from a Table (instead of a Hive Object).
 **/
public class MapOperator extends Operator<MapredWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Counter.
   *
   */
  public static enum Counter {
    DESERIALIZE_ERRORS
  }

  private final transient LongWritable deserialize_error_count = new LongWritable();
  private transient Deserializer deserializer;

  private transient Object[] rowWithPart;
  private transient Writable[] vcValues;
  private transient List<VirtualColumn> vcs;
  private transient Object[] rowWithPartAndVC;
  private transient StructObjectInspector rowObjectInspector;
  private transient boolean isPartitioned;
  private transient boolean hasVC;
  private Map<MapInputPath, MapOpCtx> opCtxMap;
  private final Set<MapInputPath> listInputPaths = new HashSet<MapInputPath>();

  private Map<Operator<? extends Serializable>, java.util.ArrayList<String>> operatorToPaths;

  private final Map<Operator<? extends Serializable>, MapOpCtx> childrenOpToOpCtxMap =
    new HashMap<Operator<? extends Serializable>, MapOpCtx>();

  private ArrayList<Operator<? extends Serializable>> extraChildrenToClose = null;

  private static class MapInputPath {
    String path;
    String alias;
    Operator<? extends Serializable> op;

    /**
     * @param path
     * @param alias
     * @param op
     */
    public MapInputPath(String path, String alias,
        Operator<? extends Serializable> op) {
      this.path = path;
      this.alias = alias;
      this.op = op;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof MapInputPath) {
        MapInputPath mObj = (MapInputPath) o;
        if (mObj == null) {
          return false;
        }
        return path.equals(mObj.path) && alias.equals(mObj.alias)
            && op.equals(mObj.op);
      }

      return false;
    }

    @Override
    public int hashCode() {
      int ret = (path == null) ? 0 : path.hashCode();
      ret += (alias == null) ? 0 : alias.hashCode();
      ret += (op == null) ? 0 : op.hashCode();
      return ret;
    }

    public Operator<? extends Serializable> getOp() {
      return op;
    }

    public void setOp(Operator<? extends Serializable> op) {
      this.op = op;
    }

  }

  private static class MapOpCtx {
    boolean isPartitioned;
    StructObjectInspector rawRowObjectInspector; // without partition
    StructObjectInspector partObjectInspector; // partition
    StructObjectInspector rowObjectInspector;
    Object[] rowWithPart;
    Object[] rowWithPartAndVC;
    Deserializer deserializer;
    public String tableName;
    public String partName;

    /**
     * @param isPartitioned
     * @param rowObjectInspector
     * @param rowWithPart
     */
    public MapOpCtx(boolean isPartitioned,
        StructObjectInspector rowObjectInspector,
        StructObjectInspector rawRowObjectInspector,
        StructObjectInspector partObjectInspector,
        Object[] rowWithPart,
        Object[] rowWithPartAndVC,
        Deserializer deserializer) {
      this.isPartitioned = isPartitioned;
      this.rowObjectInspector = rowObjectInspector;
      this.rawRowObjectInspector = rawRowObjectInspector;
      this.partObjectInspector = partObjectInspector;
      this.rowWithPart = rowWithPart;
      this.rowWithPartAndVC = rowWithPartAndVC;
      this.deserializer = deserializer;
    }

    /**
     * @return the isPartitioned
     */
    public boolean isPartitioned() {
      return isPartitioned;
    }

    /**
     * @return the rowObjectInspector
     */
    public StructObjectInspector getRowObjectInspector() {
      return rowObjectInspector;
    }

    /**
     * @return the rowWithPart
     */
    public Object[] getRowWithPart() {
      return rowWithPart;
    }

    /**
     * @return the rowWithPartAndVC
     */
    public Object[] getRowWithPartAndVC() {
      return rowWithPartAndVC;
    }

    /**
     * @return the deserializer
     */
    public Deserializer getDeserializer() {
      return deserializer;
    }
  }

  /**
   * Initializes this map op as the root of the tree. It sets JobConf &
   * MapRedWork and starts initialization of the operator tree rooted at this
   * op.
   *
   * @param hconf
   * @param mrwork
   * @throws HiveException
   */
  public void initializeAsRoot(Configuration hconf, MapredWork mrwork)
      throws HiveException {
    setConf(mrwork);
    setChildren(hconf);
    initialize(hconf, null);
  }

  private MapOpCtx initObjectInspector(MapredWork conf,
      Configuration hconf, String onefile) throws HiveException,
      ClassNotFoundException, InstantiationException, IllegalAccessException,
      SerDeException {
    PartitionDesc td = conf.getPathToPartitionInfo().get(onefile);
    LinkedHashMap<String, String> partSpec = td.getPartSpec();
    Properties tblProps = td.getProperties();

    Class sdclass = td.getDeserializerClass();
    if (sdclass == null) {
      String className = td.getSerdeClassName();
      if ((className == "") || (className == null)) {
        throw new HiveException(
            "SerDe class or the SerDe class name is not set for table: "
                + td.getProperties().getProperty("name"));
      }
      sdclass = hconf.getClassByName(className);
    }

    String tableName = String.valueOf(tblProps.getProperty("name"));
    String partName = String.valueOf(partSpec);
    // HiveConf.setVar(hconf, HiveConf.ConfVars.HIVETABLENAME, tableName);
    // HiveConf.setVar(hconf, HiveConf.ConfVars.HIVEPARTITIONNAME, partName);
    Deserializer deserializer = (Deserializer) sdclass.newInstance();
    deserializer.initialize(hconf, tblProps);
    StructObjectInspector rawRowObjectInspector = (StructObjectInspector) deserializer
        .getObjectInspector();

    MapOpCtx opCtx = null;
    // Next check if this table has partitions and if so
    // get the list of partition names as well as allocate
    // the serdes for the partition columns
    String pcols = tblProps
        .getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS);
    // Log LOG = LogFactory.getLog(MapOperator.class.getName());
    if (pcols != null && pcols.length() > 0) {
      String[] partKeys = pcols.trim().split("/");
      List<String> partNames = new ArrayList<String>(partKeys.length);
      Object[] partValues = new Object[partKeys.length];
      List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>(
          partKeys.length);
      for (int i = 0; i < partKeys.length; i++) {
        String key = partKeys[i];
        partNames.add(key);
        // Partitions do not exist for this table
        if (partSpec == null) {
          partValues[i] = new Text();
        } else {
          partValues[i] = new Text(partSpec.get(key));
        }
        partObjectInspectors
            .add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      }
      StructObjectInspector partObjectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(partNames, partObjectInspectors);

      Object[] rowWithPart = new Object[2];
      rowWithPart[1] = partValues;
      StructObjectInspector rowObjectInspector = ObjectInspectorFactory
          .getUnionStructObjectInspector(Arrays
              .asList(new StructObjectInspector[] {rawRowObjectInspector, partObjectInspector}));
      // LOG.info("dump " + tableName + " " + partName + " " +
      // rowObjectInspector.getTypeName());
      opCtx = new MapOpCtx(true, rowObjectInspector, rawRowObjectInspector, partObjectInspector,
                           rowWithPart, null, deserializer);
    } else {
      // LOG.info("dump2 " + tableName + " " + partName + " " +
      // rowObjectInspector.getTypeName());
      opCtx = new MapOpCtx(false, rawRowObjectInspector, rawRowObjectInspector, null, null,
                           null, deserializer);
    }
    opCtx.tableName = tableName;
    opCtx.partName = partName;
    return opCtx;
  }

  /**
   * Set the inspectors given a input. Since a mapper can span multiple partitions, the inspectors
   * need to be changed if the input changes
   **/
  private void setInspectorInput(MapInputPath inp) {
    Operator<? extends Serializable> op = inp.getOp();

    deserializer = opCtxMap.get(inp).getDeserializer();
    isPartitioned = opCtxMap.get(inp).isPartitioned();
    rowWithPart = opCtxMap.get(inp).getRowWithPart();
    rowWithPartAndVC = opCtxMap.get(inp).getRowWithPartAndVC();
    rowObjectInspector = opCtxMap.get(inp).getRowObjectInspector();
    if (listInputPaths.contains(inp)) {
      return;
    }

    listInputPaths.add(inp);

    if (op instanceof TableScanOperator) {
      StructObjectInspector rawRowObjectInspector = opCtxMap.get(inp).rawRowObjectInspector;
      StructObjectInspector partObjectInspector = opCtxMap.get(inp).partObjectInspector;
      TableScanOperator tsOp = (TableScanOperator) op;
      TableScanDesc tsDesc = tsOp.getConf();
      if (tsDesc != null) {
        this.vcs = tsDesc.getVirtualCols();
        if (vcs != null && vcs.size() > 0) {
          this.hasVC = true;
          List<String> vcNames = new ArrayList<String>(vcs.size());
          this.vcValues = new Writable[vcs.size()];
          List<ObjectInspector> vcsObjectInspectors = new ArrayList<ObjectInspector>(vcs.size());
          for (int i = 0; i < vcs.size(); i++) {
            VirtualColumn vc = vcs.get(i);
            vcsObjectInspectors.add(
                PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                    ((PrimitiveTypeInfo) vc.getTypeInfo()).getPrimitiveCategory()));
            vcNames.add(vc.getName());
          }
          StructObjectInspector vcStructObjectInspector = ObjectInspectorFactory
              .getStandardStructObjectInspector(vcNames,
                                              vcsObjectInspectors);
          if (isPartitioned) {
            this.rowWithPartAndVC = new Object[3];
            this.rowWithPartAndVC[1] = this.rowWithPart[1];
          } else {
            this.rowWithPartAndVC = new Object[2];
          }
          if (partObjectInspector == null) {
            this.rowObjectInspector = ObjectInspectorFactory.getUnionStructObjectInspector(Arrays
                                        .asList(new StructObjectInspector[] {
                                            rowObjectInspector, vcStructObjectInspector}));
          } else {
            this.rowObjectInspector = ObjectInspectorFactory.getUnionStructObjectInspector(Arrays
                                        .asList(new StructObjectInspector[] {
                                            rawRowObjectInspector, partObjectInspector,
                                            vcStructObjectInspector}));
          }
          opCtxMap.get(inp).rowObjectInspector = this.rowObjectInspector;
          opCtxMap.get(inp).rowWithPartAndVC = this.rowWithPartAndVC;
        }
      }
    }
  }

  public void setChildren(Configuration hconf) throws HiveException {

    Path fpath = new Path((new Path(HiveConf.getVar(hconf,
        HiveConf.ConfVars.HADOOPMAPFILENAME))).toUri().getPath());

    ArrayList<Operator<? extends Serializable>> children = new ArrayList<Operator<? extends Serializable>>();
    opCtxMap = new HashMap<MapInputPath, MapOpCtx>();
    operatorToPaths = new HashMap<Operator<? extends Serializable>, java.util.ArrayList<String>>();

    statsMap.put(Counter.DESERIALIZE_ERRORS, deserialize_error_count);

    try {
      for (String onefile : conf.getPathToAliases().keySet()) {
        MapOpCtx opCtx = initObjectInspector(conf, hconf, onefile);
        Path onepath = new Path(new Path(onefile).toUri().getPath());
        List<String> aliases = conf.getPathToAliases().get(onefile);

        for (String onealias : aliases) {
          Operator<? extends Serializable> op = conf.getAliasToWork().get(
            onealias);
          LOG.info("Adding alias " + onealias + " to work list for file "
            + onefile);
          MapInputPath inp = new MapInputPath(onefile, onealias, op);
          opCtxMap.put(inp, opCtx);
          if (operatorToPaths.get(op) == null) {
            operatorToPaths.put(op, new java.util.ArrayList<String>());
          }
          operatorToPaths.get(op).add(onefile);
          op.setParentOperators(new ArrayList<Operator<? extends Serializable>>());
          op.getParentOperators().add(this);
          // check for the operators who will process rows coming to this Map
          // Operator
          if (!onepath.toUri().relativize(fpath.toUri()).equals(fpath.toUri())) {
            children.add(op);
            childrenOpToOpCtxMap.put(op, opCtx);
            LOG.info("dump " + op.getName() + " "
                + opCtxMap.get(inp).getRowObjectInspector().getTypeName());
          }
          setInspectorInput(inp);
        }
      }

      if (children.size() == 0) {
        // didn't find match for input file path in configuration!
        // serious problem ..
        LOG.error("Configuration does not have any alias for path: "
            + fpath.toUri().getPath());
        throw new HiveException("Configuration and input path are inconsistent");
      }

      // we found all the operators that we are supposed to process.
      setChildOperators(children);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void initializeOp(Configuration hconf) throws HiveException {
    // set that parent initialization is done and call initialize on children
    state = State.INIT;
    List<Operator<? extends Serializable>> children = getChildOperators();

    for (Entry<Operator<? extends Serializable>, MapOpCtx> entry : childrenOpToOpCtxMap
        .entrySet()) {
      Operator<? extends Serializable> child = entry.getKey();
      MapOpCtx mapOpCtx = entry.getValue();
      // Add alias, table name, and partitions to hadoop conf so that their
      // children will
      // inherit these
      HiveConf.setVar(hconf, HiveConf.ConfVars.HIVETABLENAME,
          mapOpCtx.tableName);
      HiveConf.setVar(hconf, HiveConf.ConfVars.HIVEPARTITIONNAME,
          mapOpCtx.partName);
      child.initialize(hconf, new ObjectInspector[] {mapOpCtx.getRowObjectInspector()});
    }

    for (Entry<MapInputPath, MapOpCtx> entry : opCtxMap.entrySet()) {
      // Add alias, table name, and partitions to hadoop conf so that their
      // children will
      // inherit these
      HiveConf.setVar(hconf, HiveConf.ConfVars.HIVETABLENAME,
          entry.getValue().tableName);
      HiveConf.setVar(hconf, HiveConf.ConfVars.HIVEPARTITIONNAME, entry
          .getValue().partName);
      MapInputPath input = entry.getKey();
      Operator<? extends Serializable> op = input.op;
      // op is not in the children list, so need to remember it and close it
      // afterwards
      if (children.indexOf(op) == -1) {
        if (extraChildrenToClose == null) {
          extraChildrenToClose = new ArrayList<Operator<? extends Serializable>>();
        }
        extraChildrenToClose.add(op);
        op.initialize(hconf, new ObjectInspector[] {entry.getValue().getRowObjectInspector()});
      }
    }
  }

  /**
   * close extra child operators that are initialized but are not executed.
   */
  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (extraChildrenToClose != null) {
      for (Operator<? extends Serializable> op : extraChildrenToClose) {
        op.close(abort);
      }
    }
  }

  // Change the serializer etc. since it is a new file, and split can span
  // multiple files/partitions.
  @Override
  public void cleanUpInputFileChangedOp() throws HiveException {
    Path fpath = new Path((new Path(this.getExecContext().getCurrentInputFile()))
                          .toUri().getPath());

    for (String onefile : conf.getPathToAliases().keySet()) {
      Path onepath = new Path(new Path(onefile).toUri().getPath());
      // check for the operators who will process rows coming to this Map
      // Operator
      if (!onepath.toUri().relativize(fpath.toUri()).equals(fpath.toUri())) {
        String onealias = conf.getPathToAliases().get(onefile).get(0);
        Operator<? extends Serializable> op =
            conf.getAliasToWork().get(onealias);

        LOG.info("Processing alias " + onealias + " for file " + onefile);

        MapInputPath inp = new MapInputPath(onefile, onealias, op);
        setInspectorInput(inp);
        break;
      }
    }
  }

  public void process(Writable value) throws HiveException {
    // A mapper can span multiple files/partitions.
    // The serializers need to be reset if the input file changed
    if ((this.getExecContext() != null) &&
        this.getExecContext().inputFileChanged()) {
      // The child operators cleanup if input file has changed
      cleanUpInputFileChanged();
    }
    ExecMapperContext context = getExecContext();

    Object row = null;
    try {
      if (this.hasVC) {
        this.rowWithPartAndVC[0] = deserializer.deserialize(value);
        int vcPos = isPartitioned ? 2 : 1;
        if (context != null) {
          populateVirtualColumnValues(context, vcs, vcValues, deserializer);
        }
        this.rowWithPartAndVC[vcPos] = this.vcValues;
      } else if (!isPartitioned) {
        row = deserializer.deserialize((Writable) value);
      } else {
        rowWithPart[0] = deserializer.deserialize((Writable) value);
      }
    } catch (Exception e) {
      // Serialize the row and output.
      String rawRowString;
      try {
        rawRowString = value.toString();
      } catch (Exception e2) {
        rawRowString = "[Error getting row data with exception " +
            StringUtils.stringifyException(e2) + " ]";
      }

      // TODO: policy on deserialization errors
      deserialize_error_count.set(deserialize_error_count.get() + 1);
      throw new HiveException("Hive Runtime Error while processing writable " + rawRowString, e);
    }

    try {
      if (this.hasVC) {
        forward(this.rowWithPartAndVC, this.rowObjectInspector);
      } else if (!isPartitioned) {
        forward(row, rowObjectInspector);
      } else {
        forward(rowWithPart, rowObjectInspector);
      }
    } catch (Exception e) {
      // Serialize the row and output the error message.
      String rowString;
      try {
        if (this.hasVC) {
          rowString = SerDeUtils.getJSONString(rowWithPartAndVC, rowObjectInspector);
        } else if (!isPartitioned) {
          rowString = SerDeUtils.getJSONString(row, rowObjectInspector);
        } else {
          rowString = SerDeUtils.getJSONString(rowWithPart, rowObjectInspector);
        }
      } catch (Exception e2) {
        rowString = "[Error getting row data with exception " +
            StringUtils.stringifyException(e2) + " ]";
      }
      throw new HiveException("Hive Runtime Error while processing row " + rowString, e);
    }
  }

  public static Writable[] populateVirtualColumnValues(ExecMapperContext ctx,
      List<VirtualColumn> vcs, Writable[] vcValues, Deserializer deserializer) {
    if (vcs == null) {
      return vcValues;
    }
    if (vcValues == null) {
      vcValues = new Writable[vcs.size()];
    }
    for (int i = 0; i < vcs.size(); i++) {
      VirtualColumn vc = vcs.get(i);
      if (vc.equals(VirtualColumn.FILENAME)) {
        if (ctx.inputFileChanged()) {
          vcValues[i] = new Text(ctx.getCurrentInputFile());
        }
      } else if (vc.equals(VirtualColumn.BLOCKOFFSET)) {
        long current = ctx.getIoCxt().getCurrentBlockStart();
        LongWritable old = (LongWritable) vcValues[i];
        if (old == null) {
          old = new LongWritable(current);
          vcValues[i] = old;
          continue;
        }
        if (current != old.get()) {
          old.set(current);
        }
      } else if (vc.equals(VirtualColumn.ROWOFFSET)) {
        long current = ctx.getIoCxt().getCurrentRow();
        LongWritable old = (LongWritable) vcValues[i];
        if (old == null) {
          old = new LongWritable(current);
          vcValues[i] = old;
          continue;
        }
        if (current != old.get()) {
          old.set(current);
        }
      } else if (vc.equals(VirtualColumn.RAWDATASIZE)) {
        long current = 0L;
        SerDeStats stats = deserializer.getSerDeStats();
        if(stats != null) {
          current = stats.getRawDataSize();
        }
        LongWritable old = (LongWritable) vcValues[i];
        if (old == null) {
          old = new LongWritable(current);
          vcValues[i] = old;
          continue;
        }
        if (current != old.get()) {
          old.set(current);
        }
      }
    }
    return vcValues;
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    throw new HiveException("Hive 2 Internal error: should not be called!");
  }

  @Override
  public String getName() {
    return "MAP";
  }

  @Override
  public OperatorType getType() {
    return null;
  }

}
