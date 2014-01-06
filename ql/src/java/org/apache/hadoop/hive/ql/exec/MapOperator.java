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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.IOContext;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

/**
 * Map operator. This triggers overall map side processing. This is a little
 * different from regular operators in that it starts off by processing a
 * Writable data structure from a Table (instead of a Hive Object).
 **/
public class MapOperator extends Operator<MapWork> implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;

  /**
   * Counter.
   *
   */
  public static enum Counter {
    DESERIALIZE_ERRORS
  }

  private final transient LongWritable deserialize_error_count = new LongWritable();

  private final Map<MapInputPath, MapOpCtx> opCtxMap = new HashMap<MapInputPath, MapOpCtx>();
  private final Map<Operator<? extends OperatorDesc>, MapOpCtx> childrenOpToOpCtxMap =
    new HashMap<Operator<? extends OperatorDesc>, MapOpCtx>();

  protected transient MapOpCtx current;
  private transient List<Operator<? extends OperatorDesc>> extraChildrenToClose = null;
  private final Map<String, Path> normalizedPaths = new HashMap<String, Path>();

  private static class MapInputPath {
    String path;
    String alias;
    Operator<?> op;
    PartitionDesc partDesc;

    /**
     * @param path
     * @param alias
     * @param op
     */
    public MapInputPath(String path, String alias, Operator<?> op, PartitionDesc partDesc) {
      this.path = path;
      this.alias = alias;
      this.op = op;
      this.partDesc = partDesc;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof MapInputPath) {
        MapInputPath mObj = (MapInputPath) o;
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
  }

  protected static class MapOpCtx {

    StructObjectInspector tblRawRowObjectInspector;  // columns
    StructObjectInspector partObjectInspector;    // partition columns
    StructObjectInspector vcsObjectInspector;     // virtual columns
    StructObjectInspector rowObjectInspector;

    Converter partTblObjectInspectorConverter;

    Object[] rowWithPart;
    Object[] rowWithPartAndVC;
    Deserializer deserializer;

    String tableName;
    String partName;
    List<VirtualColumn> vcs;
    Writable[] vcValues;

    private boolean isPartitioned() {
      return partObjectInspector != null;
    }

    private boolean hasVC() {
      return vcsObjectInspector != null;
    }

    private Object readRow(Writable value) throws SerDeException {
      return partTblObjectInspectorConverter.convert(deserializer.deserialize(value));
    }

    public StructObjectInspector getRowObjectInspector() {
      return rowObjectInspector;
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
  public void initializeAsRoot(Configuration hconf, MapWork mapWork)
      throws HiveException {
    setConf(mapWork);
    setChildren(hconf);
    initialize(hconf, null);
  }

  private MapOpCtx initObjectInspector(Configuration hconf, MapInputPath ctx,
      Map<TableDesc, StructObjectInspector> convertedOI) throws Exception {

    PartitionDesc pd = ctx.partDesc;
    TableDesc td = pd.getTableDesc();

    MapOpCtx opCtx = new MapOpCtx();
    // Use table properties in case of unpartitioned tables,
    // and the union of table properties and partition properties, with partition
    // taking precedence
    Properties partProps = isPartitioned(pd) ?
        pd.getOverlayedProperties() : pd.getTableDesc().getProperties();

    Map<String, String> partSpec = pd.getPartSpec();

    opCtx.tableName = String.valueOf(partProps.getProperty("name"));
    opCtx.partName = String.valueOf(partSpec);

    Class serdeclass = hconf.getClassByName(pd.getSerdeClassName());
    opCtx.deserializer = (Deserializer) serdeclass.newInstance();
    opCtx.deserializer.initialize(hconf, partProps);

    StructObjectInspector partRawRowObjectInspector =
        (StructObjectInspector) opCtx.deserializer.getObjectInspector();

    opCtx.tblRawRowObjectInspector = convertedOI.get(td);

    opCtx.partTblObjectInspectorConverter = ObjectInspectorConverters.getConverter(
        partRawRowObjectInspector, opCtx.tblRawRowObjectInspector);

    // Next check if this table has partitions and if so
    // get the list of partition names as well as allocate
    // the serdes for the partition columns
    String pcols = partProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    // Log LOG = LogFactory.getLog(MapOperator.class.getName());
    if (pcols != null && pcols.length() > 0) {
      String[] partKeys = pcols.trim().split("/");
      List<String> partNames = new ArrayList<String>(partKeys.length);
      Object[] partValues = new Object[partKeys.length];
      List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>(partKeys.length);
      for (int i = 0; i < partKeys.length; i++) {
        String key = partKeys[i];
        partNames.add(key);
        // Partitions do not exist for this table
        if (partSpec == null) {
          // for partitionless table, initialize partValue to null
          partValues[i] = null;
        } else {
          partValues[i] = new Text(partSpec.get(key));
        }
        partObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      }
      opCtx.rowWithPart = new Object[] {null, partValues};
      opCtx.partObjectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(partNames, partObjectInspectors);
    }

    // The op may not be a TableScan for mapjoins
    // Consider the query: select /*+MAPJOIN(a)*/ count(*) FROM T1 a JOIN T2 b ON a.key = b.key;
    // In that case, it will be a Select, but the rowOI need not be ammended
    if (ctx.op instanceof TableScanOperator) {
      TableScanOperator tsOp = (TableScanOperator) ctx.op;
      TableScanDesc tsDesc = tsOp.getConf();
      if (tsDesc != null && tsDesc.hasVirtualCols()) {
        opCtx.vcs = tsDesc.getVirtualCols();
        opCtx.vcValues = new Writable[opCtx.vcs.size()];
        opCtx.vcsObjectInspector = VirtualColumn.getVCSObjectInspector(opCtx.vcs);
        if (opCtx.isPartitioned()) {
          opCtx.rowWithPartAndVC = Arrays.copyOfRange(opCtx.rowWithPart, 0, 3);
        } else {
          opCtx.rowWithPartAndVC = new Object[2];
        }
      }
    }
    if (!opCtx.hasVC() && !opCtx.isPartitioned()) {
      opCtx.rowObjectInspector = opCtx.tblRawRowObjectInspector;
      return opCtx;
    }
    List<StructObjectInspector> inspectors = new ArrayList<StructObjectInspector>();
    inspectors.add(opCtx.tblRawRowObjectInspector);
    if (opCtx.isPartitioned()) {
      inspectors.add(opCtx.partObjectInspector);
    }
    if (opCtx.hasVC()) {
      inspectors.add(opCtx.vcsObjectInspector);
    }
    opCtx.rowObjectInspector = ObjectInspectorFactory.getUnionStructObjectInspector(inspectors);
    return opCtx;
  }

  // Return the mapping for table descriptor to the expected table OI
  /**
   * Traverse all the partitions for a table, and get the OI for the table.
   * Note that a conversion is required if any of the partition OI is different
   * from the table OI. For eg. if the query references table T (partitions P1, P2),
   * and P1's schema is same as T, whereas P2's scheme is different from T, conversion
   * might be needed for both P1 and P2, since SettableOI might be needed for T
   */
  private Map<TableDesc, StructObjectInspector> getConvertedOI(Configuration hconf)
      throws HiveException {
    Map<TableDesc, StructObjectInspector> tableDescOI =
        new HashMap<TableDesc, StructObjectInspector>();
    Set<TableDesc> identityConverterTableDesc = new HashSet<TableDesc>();
    try {
      Map<ObjectInspector, Boolean> oiSettableProperties = new HashMap<ObjectInspector, Boolean>();

      for (String onefile : conf.getPathToAliases().keySet()) {
        PartitionDesc pd = conf.getPathToPartitionInfo().get(onefile);
        TableDesc tableDesc = pd.getTableDesc();
        Properties tblProps = tableDesc.getProperties();
        // If the partition does not exist, use table properties
        Properties partProps = isPartitioned(pd) ? pd.getOverlayedProperties() : tblProps;
        Class sdclass = hconf.getClassByName(pd.getSerdeClassName());
        Deserializer partDeserializer = (Deserializer) sdclass.newInstance();
        partDeserializer.initialize(hconf, partProps);
        StructObjectInspector partRawRowObjectInspector = (StructObjectInspector) partDeserializer
            .getObjectInspector();

        StructObjectInspector tblRawRowObjectInspector = tableDescOI.get(tableDesc);
        if ((tblRawRowObjectInspector == null) ||
            (identityConverterTableDesc.contains(tableDesc))) {
            sdclass = hconf.getClassByName(tableDesc.getSerdeClassName());
            Deserializer tblDeserializer = (Deserializer) sdclass.newInstance();
          tblDeserializer.initialize(hconf, tblProps);
          tblRawRowObjectInspector =
              (StructObjectInspector) ObjectInspectorConverters.getConvertedOI(
                  partRawRowObjectInspector,
                  tblDeserializer.getObjectInspector(), oiSettableProperties);

          if (identityConverterTableDesc.contains(tableDesc)) {
            if (!partRawRowObjectInspector.equals(tblRawRowObjectInspector)) {
              identityConverterTableDesc.remove(tableDesc);
            }
          }
          else if (partRawRowObjectInspector.equals(tblRawRowObjectInspector)) {
            identityConverterTableDesc.add(tableDesc);
          }

          tableDescOI.put(tableDesc, tblRawRowObjectInspector);
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return tableDescOI;
  }

  private boolean isPartitioned(PartitionDesc pd) {
    return pd.getPartSpec() != null && !pd.getPartSpec().isEmpty();
  }

  public void setChildren(Configuration hconf) throws HiveException {

    Path fpath = IOContext.get().getInputPath();

    boolean schemeless = fpath.toUri().getScheme() == null;

    List<Operator<? extends OperatorDesc>> children =
        new ArrayList<Operator<? extends OperatorDesc>>();

    Map<TableDesc, StructObjectInspector> convertedOI = getConvertedOI(hconf);

    try {
      for (Map.Entry<String, ArrayList<String>> entry : conf.getPathToAliases().entrySet()) {
        String onefile = entry.getKey();
        List<String> aliases = entry.getValue();

        Path onepath = new Path(onefile);
        if (schemeless) {
          onepath = new Path(onepath.toUri().getPath());
        }

        PartitionDesc partDesc = conf.getPathToPartitionInfo().get(onefile);

        for (String onealias : aliases) {
          Operator<? extends OperatorDesc> op = conf.getAliasToWork().get(onealias);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding alias " + onealias + " to work list for file "
               + onefile);
          }
          MapInputPath inp = new MapInputPath(onefile, onealias, op, partDesc);
          if (opCtxMap.containsKey(inp)) {
            continue;
          }
          MapOpCtx opCtx = initObjectInspector(hconf, inp, convertedOI);
          opCtxMap.put(inp, opCtx);

          op.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>());
          op.getParentOperators().add(this);
          // check for the operators who will process rows coming to this Map
          // Operator
          if (!onepath.toUri().relativize(fpath.toUri()).equals(fpath.toUri())) {
            children.add(op);
            childrenOpToOpCtxMap.put(op, opCtx);
            LOG.info("dump " + op.getName() + " "
                + opCtxMap.get(inp).rowObjectInspector.getTypeName());
          }
          current = opCtx;  // just need for TestOperators.testMapOperator
        }
      }

      if (children.size() == 0) {
        // didn't find match for input file path in configuration!
        // serious problem ..
        LOG.error("Configuration does not have any alias for path: "
            + fpath.toUri());
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
    statsMap.put(Counter.DESERIALIZE_ERRORS, deserialize_error_count);

    List<Operator<? extends OperatorDesc>> children = getChildOperators();

    for (Entry<Operator<? extends OperatorDesc>, MapOpCtx> entry : childrenOpToOpCtxMap
        .entrySet()) {
      Operator<? extends OperatorDesc> child = entry.getKey();
      MapOpCtx mapOpCtx = entry.getValue();
      // Add alias, table name, and partitions to hadoop conf so that their
      // children will inherit these
      HiveConf.setVar(hconf, HiveConf.ConfVars.HIVETABLENAME, mapOpCtx.tableName);
      HiveConf.setVar(hconf, HiveConf.ConfVars.HIVEPARTITIONNAME, mapOpCtx.partName);
      child.initialize(hconf, new ObjectInspector[] {mapOpCtx.rowObjectInspector});
    }

    for (Entry<MapInputPath, MapOpCtx> entry : opCtxMap.entrySet()) {
      MapInputPath input = entry.getKey();
      MapOpCtx mapOpCtx = entry.getValue();
      // Add alias, table name, and partitions to hadoop conf so that their
      // children will inherit these
      HiveConf.setVar(hconf, HiveConf.ConfVars.HIVETABLENAME, mapOpCtx.tableName);
      HiveConf.setVar(hconf, HiveConf.ConfVars.HIVEPARTITIONNAME, mapOpCtx.partName);

      Operator<? extends OperatorDesc> op = input.op;
      if (children.indexOf(op) == -1) {
        // op is not in the children list, so need to remember it and close it afterwards
        if (extraChildrenToClose == null) {
          extraChildrenToClose = new ArrayList<Operator<? extends OperatorDesc>>();
        }
        extraChildrenToClose.add(op);
        op.initialize(hconf, new ObjectInspector[] {entry.getValue().rowObjectInspector});
      }
    }
  }

  /**
   * close extra child operators that are initialized but are not executed.
   */
  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (extraChildrenToClose != null) {
      for (Operator<? extends OperatorDesc> op : extraChildrenToClose) {
        op.close(abort);
      }
    }
  }

  // Find context for current input file
  @Override
  public void cleanUpInputFileChangedOp() throws HiveException {
    Path fpath = getExecContext().getCurrentInputPath();

    for (String onefile : conf.getPathToAliases().keySet()) {
      Path onepath = normalizePath(onefile);
      // check for the operators who will process rows coming to this Map
      // Operator
      if (onepath.toUri().relativize(fpath.toUri()).equals(fpath.toUri())) {
        // not from this
        continue;
      }
      PartitionDesc partDesc = conf.getPathToPartitionInfo().get(onefile);
      for (String onealias : conf.getPathToAliases().get(onefile)) {
        Operator<? extends OperatorDesc> op = conf.getAliasToWork().get(onealias);
        MapInputPath inp = new MapInputPath(onefile, onealias, op, partDesc);
        MapOpCtx context = opCtxMap.get(inp);
        if (context != null) {
          current = context;
          LOG.info("Processing alias " + onealias + " for file " + onefile);
          return;
        }
      }
    }
    throw new IllegalStateException("Invalid path " + fpath);
  }

  private Path normalizePath(String onefile) {
    //creating Path is expensive, so cache the corresponding
    //Path object in normalizedPaths
    Path path = normalizedPaths.get(onefile);
    if(path == null){
      path = new Path(onefile);
      normalizedPaths.put(onefile, path);
    }
    return path;
  }

  public void process(Writable value) throws HiveException {
    // A mapper can span multiple files/partitions.
    // The serializers need to be reset if the input file changed
    ExecMapperContext context = getExecContext();
    if (context != null && context.inputFileChanged()) {
      // The child operators cleanup if input file has changed
      cleanUpInputFileChanged();
    }
    Object row;
    try {
      row = current.readRow(value);
      if (current.hasVC()) {
        current.rowWithPartAndVC[0] = row;
        if (context != null) {
          populateVirtualColumnValues(context, current.vcs, current.vcValues, current.deserializer);
        }
        int vcPos = current.isPartitioned() ? 2 : 1;
        current.rowWithPartAndVC[vcPos] = current.vcValues;
        row = current.rowWithPartAndVC;
      } else if (current.isPartitioned()) {
        current.rowWithPart[0] = row;
        row = current.rowWithPart;
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

    // The row has been converted to comply with table schema, irrespective of partition schema.
    // So, use tblOI (and not partOI) for forwarding
    try {
      forward(row, current.rowObjectInspector);
    } catch (Exception e) {
      // Serialize the row and output the error message.
      String rowString;
      try {
        rowString = SerDeUtils.getJSONString(row, current.rowObjectInspector);
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
          vcValues[i] = new Text(ctx.getCurrentInputPath().toString());
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
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "MAP";
  }

  @Override
  public OperatorType getType() {
    return null;
  }

}
