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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.tez.MapRecordProcessor;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Map operator. This triggers overall map side processing. This is a little
 * different from regular operators in that it starts off by processing a
 * Writable data structure from a Table (instead of a Hive Object).
 **/
@SuppressWarnings("deprecation")
public class MapOperator extends Operator<MapWork> implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;

  /**
   * Counter.
   *
   */
  public static enum Counter {
    DESERIALIZE_ERRORS,
    RECORDS_IN
  }

  private final transient LongWritable deserialize_error_count = new LongWritable();
  private final transient LongWritable recordCounter = new LongWritable();
  protected transient long numRows = 0;
  protected transient long cntr = 1;
  protected transient long logEveryNRows = 0;

  // input path --> {operator --> context}
  private final Map<String, Map<Operator<?>, MapOpCtx>> opCtxMap =
      new HashMap<String, Map<Operator<?>, MapOpCtx>>();
  // child operator --> object inspector (converted OI if it's needed)
  private final Map<Operator<?>, StructObjectInspector> childrenOpToOI =
      new HashMap<Operator<?>, StructObjectInspector>();

  // context for current input file
  protected transient MapOpCtx[] currentCtxs;
  private transient final Map<String, Path> normalizedPaths = new HashMap<String, Path>();

  protected static class MapOpCtx {

    final String alias;
    final Operator<?> op;
    final PartitionDesc partDesc;

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
    Object[] vcValues;

    public MapOpCtx(String alias, Operator<?> op, PartitionDesc partDesc) {
      this.alias = alias;
      this.op = op;
      this.partDesc = partDesc;
    }

    private boolean isPartitioned() {
      return partObjectInspector != null;
    }

    private boolean hasVC() {
      return vcsObjectInspector != null;
    }

    private Object readRow(Writable value, ExecMapperContext context) throws SerDeException {
      Object deserialized = deserializer.deserialize(value);
      Object row = partTblObjectInspectorConverter.convert(deserialized);
      if (hasVC()) {
        rowWithPartAndVC[0] = row;
        if (context != null) {
          populateVirtualColumnValues(context, vcs, vcValues, deserializer);
        }
        int vcPos = isPartitioned() ? 2 : 1;
        rowWithPartAndVC[vcPos] = vcValues;
        return  rowWithPartAndVC;
      } else if (isPartitioned()) {
        rowWithPart[0] = row;
        return rowWithPart;
      }
      return row;
    }

    public boolean forward(Object row) throws HiveException {
      if (op.getDone()) {
        return false;
      }
      op.process(row, 0);
      return true;
    }
  }

  /**
   * Initializes this map op as the root of the tree. It sets JobConf &
   * MapRedWork and starts initialization of the operator tree rooted at this
   * op.
   *
   * @param hconf
   * @param mapWork
   * @throws HiveException
   */
  @VisibleForTesting
  void initializeAsRoot(JobConf hconf, MapWork mapWork) throws Exception {
    setConf(mapWork);
    setChildren(hconf);
    passExecContext(new ExecMapperContext(hconf));
    initializeMapOperator(hconf);
  }

  private MapOpCtx initObjectInspector(Configuration hconf, MapOpCtx opCtx,
      StructObjectInspector tableRowOI) throws Exception {
    PartitionDesc pd = opCtx.partDesc;
    TableDesc td = pd.getTableDesc();

    // Use table properties in case of unpartitioned tables,
    // and the union of table properties and partition properties, with partition
    // taking precedence, in the case of partitioned tables
    Properties overlayedProps =
        SerDeUtils.createOverlayedProperties(td.getProperties(), pd.getProperties());

    Map<String, String> partSpec = pd.getPartSpec();

    opCtx.tableName = String.valueOf(overlayedProps.getProperty("name"));
    opCtx.partName = String.valueOf(partSpec);
    opCtx.deserializer = pd.getDeserializer(hconf);

    StructObjectInspector partRawRowObjectInspector =
        (StructObjectInspector) opCtx.deserializer.getObjectInspector();

    opCtx.partTblObjectInspectorConverter =
        ObjectInspectorConverters.getConverter(partRawRowObjectInspector, tableRowOI);

    // Next check if this table has partitions and if so
    // get the list of partition names as well as allocate
    // the serdes for the partition columns
    String pcols = overlayedProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);

    if (pcols != null && pcols.length() > 0) {
      String[] partKeys = pcols.trim().split("/");
      String pcolTypes = overlayedProps
          .getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
      String[] partKeyTypes = pcolTypes.trim().split(":");

      if (partKeys.length > partKeyTypes.length) {
          throw new HiveException("Internal error : partKeys length, " +partKeys.length +
                  " greater than partKeyTypes length, " + partKeyTypes.length);
      }

      List<String> partNames = new ArrayList<String>(partKeys.length);
      Object[] partValues = new Object[partKeys.length];
      List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>(partKeys.length);

      for (int i = 0; i < partKeys.length; i++) {
        String key = partKeys[i];
        partNames.add(key);
        ObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector
            (TypeInfoFactory.getPrimitiveTypeInfo(partKeyTypes[i]));

        // Partitions do not exist for this table
        if (partSpec == null) {
          // for partitionless table, initialize partValue to null
          partValues[i] = null;
        } else {
            partValues[i] =
                ObjectInspectorConverters.
                getConverter(PrimitiveObjectInspectorFactory.
                    javaStringObjectInspector, oi).convert(partSpec.get(key));
        }
        partObjectInspectors.add(oi);
      }
      opCtx.rowWithPart = new Object[] {null, partValues};
      opCtx.partObjectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(partNames, partObjectInspectors);
    }

    // The op may not be a TableScan for mapjoins
    // Consider the query: select /*+MAPJOIN(a)*/ count(*) FROM T1 a JOIN T2 b ON a.key = b.key;
    // In that case, it will be a Select, but the rowOI need not be amended
    if (opCtx.op instanceof TableScanOperator) {
      TableScanOperator tsOp = (TableScanOperator) opCtx.op;
      TableScanDesc tsDesc = tsOp.getConf();
      if (tsDesc != null && tsDesc.hasVirtualCols()) {
        opCtx.vcs = tsDesc.getVirtualCols();
        opCtx.vcValues = new Object[opCtx.vcs.size()];
        opCtx.vcsObjectInspector = VirtualColumn.getVCSObjectInspector(opCtx.vcs);
        if (opCtx.isPartitioned()) {
          opCtx.rowWithPartAndVC = Arrays.copyOfRange(opCtx.rowWithPart, 0, 3);
        } else {
          opCtx.rowWithPartAndVC = new Object[2];
        }
      }
    }
    if (!opCtx.hasVC() && !opCtx.isPartitioned()) {
      opCtx.rowObjectInspector = tableRowOI;
      return opCtx;
    }
    List<StructObjectInspector> inspectors = new ArrayList<StructObjectInspector>();
    inspectors.add(tableRowOI);
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
        Deserializer partDeserializer = pd.getDeserializer(hconf);
        StructObjectInspector partRawRowObjectInspector =
            (StructObjectInspector) partDeserializer.getObjectInspector();

        StructObjectInspector tblRawRowObjectInspector = tableDescOI.get(tableDesc);
        if ((tblRawRowObjectInspector == null) ||
            (identityConverterTableDesc.contains(tableDesc))) {
          Deserializer tblDeserializer = tableDesc.getDeserializer(hconf);
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

  public void setChildren(Configuration hconf) throws Exception {

    List<Operator<? extends OperatorDesc>> children =
        new ArrayList<Operator<? extends OperatorDesc>>();

    Map<TableDesc, StructObjectInspector> convertedOI = getConvertedOI(hconf);

    for (Map.Entry<String, ArrayList<String>> entry : conf.getPathToAliases().entrySet()) {
      String onefile = entry.getKey();
      List<String> aliases = entry.getValue();
      PartitionDesc partDesc = conf.getPathToPartitionInfo().get(onefile);

      for (String alias : aliases) {
        Operator<? extends OperatorDesc> op = conf.getAliasToWork().get(alias);
        if (isLogDebugEnabled) {
          LOG.debug("Adding alias " + alias + " to work list for file "
              + onefile);
        }
        Map<Operator<?>, MapOpCtx> contexts = opCtxMap.get(onefile);
        if (contexts == null) {
          opCtxMap.put(onefile, contexts = new LinkedHashMap<Operator<?>, MapOpCtx>());
        }
        if (contexts.containsKey(op)) {
          continue;
        }
        MapOpCtx context = new MapOpCtx(alias, op, partDesc);
        StructObjectInspector tableRowOI = convertedOI.get(partDesc.getTableDesc());
        contexts.put(op, initObjectInspector(hconf, context, tableRowOI));

        if (children.contains(op) == false) {
          op.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>(1));
          op.getParentOperators().add(this);
          children.add(op);
        }
      }
    }

    initOperatorContext(children);

    // we found all the operators that we are supposed to process.
    setChildOperators(children);
  }

  private void initOperatorContext(List<Operator<? extends OperatorDesc>> children)
      throws HiveException {
    for (Map<Operator<?>, MapOpCtx> contexts : opCtxMap.values()) {
      for (MapOpCtx context : contexts.values()) {
        if (!children.contains(context.op)) {
          continue;
        }
        StructObjectInspector prev =
            childrenOpToOI.put(context.op, context.rowObjectInspector);
        if (prev != null && !prev.equals(context.rowObjectInspector)) {
          throw new HiveException("Conflict on row inspector for " + context.alias);
        }
        if (isLogDebugEnabled) {
          LOG.debug("dump " + context.op + " " + context.rowObjectInspector.getTypeName());
        }
      }
    }
  }

  private String getNominalPath(Path fpath) {
    String nominal = null;
    boolean schemaless = fpath.toUri().getScheme() == null;
    for (String onefile : conf.getPathToAliases().keySet()) {
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
    }
    if (nominal == null) {
      throw new IllegalStateException("Invalid input path " + fpath);
    }
    return nominal;
  }

  @Override
  public Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    return super.initializeOp(hconf);
  }

  public void initializeMapOperator(Configuration hconf) throws HiveException {
    // set that parent initialization is done and call initialize on children
    state = State.INIT;
    statsMap.put(Counter.DESERIALIZE_ERRORS.toString(), deserialize_error_count);

    numRows = 0;
    cntr = 1;
    logEveryNRows = HiveConf.getLongVar(hconf, HiveConf.ConfVars.HIVE_LOG_N_RECORDS);

    String context = hconf.get(Operator.CONTEXT_NAME_KEY, "");
    if (context != null && !context.isEmpty()) {
      context = "_" + context.replace(" ","_");
    }
    statsMap.put(Counter.RECORDS_IN + context, recordCounter);

    for (Entry<Operator<?>, StructObjectInspector> entry : childrenOpToOI.entrySet()) {
      Operator<?> child = entry.getKey();
      child.initialize(hconf, new ObjectInspector[] {entry.getValue()});
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    recordCounter.set(numRows);
    super.closeOp(abort);
  }

  // Find context for current input file
  @Override
  public void cleanUpInputFileChangedOp() throws HiveException {
    super.cleanUpInputFileChangedOp();
    Path fpath = getExecContext().getCurrentInputPath();
    String nominalPath = getNominalPath(fpath);
    Map<Operator<?>, MapOpCtx> contexts = opCtxMap.get(nominalPath);
    if (isLogInfoEnabled) {
      StringBuilder builder = new StringBuilder();
      for (MapOpCtx context : contexts.values()) {
        if (builder.length() > 0) {
          builder.append(", ");
        }
        builder.append(context.alias);
      }
      if (isLogDebugEnabled) {
        LOG.debug("Processing alias(es) " + builder.toString() + " for file " + fpath);
      }
    }
    // Add alias, table name, and partitions to hadoop conf so that their
    // children will inherit these
    for (Entry<Operator<?>, MapOpCtx> entry : contexts.entrySet()) {
      Operator<?> operator = entry.getKey();
      MapOpCtx context = entry.getValue();
      operator.setInputContext(nominalPath, context.tableName, context.partName);
    }
    currentCtxs = contexts.values().toArray(new MapOpCtx[contexts.size()]);
  }

  private Path normalizePath(String onefile, boolean schemaless) {
    //creating Path is expensive, so cache the corresponding
    //Path object in normalizedPaths
    Path path = normalizedPaths.get(onefile);
    if (path == null) {
      path = new Path(onefile);
      if (schemaless && path.toUri().getScheme() != null) {
        path = new Path(path.toUri().getPath());
      }
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
    int childrenDone = 0;
    for (MapOpCtx current : currentCtxs) {
      Object row = null;
      try {
        row = current.readRow(value, context);
        if (!current.forward(row)) {
          childrenDone++;
        }
      } catch (Exception e) {
        // TODO: policy on deserialization errors
        String message = toErrorMessage(value, row, current.rowObjectInspector);
        if (row == null) {
          deserialize_error_count.set(deserialize_error_count.get() + 1);
          throw new HiveException("Hive Runtime Error while processing writable " + message, e);
        }
        throw new HiveException("Hive Runtime Error while processing row " + message, e);
      }
    }
    rowsForwarded(childrenDone, 1);
  }

  protected final void rowsForwarded(int childrenDone, int rows) {
    numRows += rows;
    if (isLogInfoEnabled) {
      while (numRows >= cntr) {
        cntr = logEveryNRows == 0 ? cntr * 10 : numRows + logEveryNRows;
        if (cntr < 0 || numRows < 0) {
          cntr = 1;
          numRows = 0;
        }
        LOG.info(toString() + ": records read - " + numRows);
      }
    }
    if (childrenDone == currentCtxs.length) {
      setDone(true);
    }
  }

  private String toErrorMessage(Writable value, Object row, ObjectInspector inspector) {
    try {
      if (row != null) {
        return SerDeUtils.getJSONString(row, inspector);
      }
      return String.valueOf(value);
    } catch (Exception e) {
      return "[Error getting row data with exception " + StringUtils.stringifyException(e) + " ]";
    }
  }

  public static Object[] populateVirtualColumnValues(ExecMapperContext ctx,
      List<VirtualColumn> vcs, Object[] vcValues, Deserializer deserializer) {
    if (vcs == null) {
      return vcValues;
    }
    if (vcValues == null) {
      vcValues = new Object[vcs.size()];
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
      else if(vc.equals(VirtualColumn.ROWID)) {
        if(ctx.getIoCxt().getRecordIdentifier() == null) {
          vcValues[i] = null;
        }
        else {
          if(vcValues[i] == null) {
            vcValues[i] = new Object[RecordIdentifier.Field.values().length];
          }
          RecordIdentifier.StructInfo.toArray(ctx.getIoCxt().getRecordIdentifier(), (Object[])vcValues[i]);
          ctx.getIoCxt().setRecordIdentifier(null);//so we don't accidentally cache the value; shouldn't
          //happen since IO layer either knows how to produce ROW__ID or not - but to be safe
        }
      }
    }
    return vcValues;
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
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

  public void initializeContexts() {
    Path fpath = getExecContext().getCurrentInputPath();
    String nominalPath = getNominalPath(fpath);
    Map<Operator<?>, MapOpCtx> contexts = opCtxMap.get(nominalPath);
    currentCtxs = contexts.values().toArray(new MapOpCtx[contexts.size()]);
  }

  public Deserializer getCurrentDeserializer() {

    return currentCtxs[0].deserializer;
  }
}
