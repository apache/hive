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
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
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
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
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
public class MapOperator extends AbstractMapOperator {

  private static final long serialVersionUID = 1L;

  protected transient long cntr = 1;
  protected transient long logEveryNRows = 0;

  // input path --> {operator --> context}
  private final Map<Path, Map<Operator<?>, MapOpCtx>> opCtxMap = new HashMap<>();
  // child operator --> object inspector (converted OI if it's needed)
  private final Map<Operator<?>, StructObjectInspector> childrenOpToOI =
      new HashMap<Operator<?>, StructObjectInspector>();

  // context for current input file
  protected transient MapOpCtx[] currentCtxs;

  protected static class MapOpCtx {

    @Override
    public String toString() {
      return "[alias=" + alias + ", op=" + op + "]";
    }

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

    StructObjectInspector partRawRowObjectInspector;
    boolean isAcid = AcidUtils.isTablePropertyTransactional(td.getProperties());
    if (Utilities.isSchemaEvolutionEnabled(hconf, isAcid) && Utilities.isInputFileFormatSelfDescribing(pd)) {
      partRawRowObjectInspector = tableRowOI;
    } else {
      partRawRowObjectInspector =
          (StructObjectInspector) opCtx.deserializer.getObjectInspector();
    }

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
  private Map<TableDesc, StructObjectInspector> getConvertedOI(Map<String, Configuration> tableToConf)
      throws HiveException {
    Map<TableDesc, StructObjectInspector> tableDescOI =
        new HashMap<TableDesc, StructObjectInspector>();
    Set<TableDesc> identityConverterTableDesc = new HashSet<TableDesc>();

    try {
      Map<ObjectInspector, Boolean> oiSettableProperties = new HashMap<ObjectInspector, Boolean>();

      for (Path onefile : conf.getPathToAliases().keySet()) {
        PartitionDesc pd = conf.getPathToPartitionInfo().get(onefile);
        TableDesc tableDesc = pd.getTableDesc();
        Configuration hconf = tableToConf.get(tableDesc.getFullTableName());
        Deserializer partDeserializer = pd.getDeserializer(hconf);
        StructObjectInspector partRawRowObjectInspector;
        boolean isAcid = AcidUtils.isTablePropertyTransactional(tableDesc.getProperties());
        if (Utilities.isSchemaEvolutionEnabled(hconf, isAcid) && Utilities.isInputFileFormatSelfDescribing(pd)) {
          Deserializer tblDeserializer = tableDesc.getDeserializer(hconf);
          partRawRowObjectInspector = (StructObjectInspector) tblDeserializer.getObjectInspector();
        } else {
          partRawRowObjectInspector =
              (StructObjectInspector) partDeserializer.getObjectInspector();
        }

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

  /**
   * For each source table, combine the nested column pruning information from all its
   * table scan descriptors and set it in a configuration copy. This is necessary since
   * the configuration properties are set on a per-table basis, so we can't just use a
   * single configuration for all the tables.
   */
  private Map<String, Configuration> cloneConfsForColPruning(Configuration hconf) {
    Map<String, Configuration> tableNameToConf = new HashMap<>();

    for (Map.Entry<Path, List<String>> e : conf.getPathToAliases().entrySet()) {
      List<String> aliases = e.getValue();
      if (aliases == null || aliases.isEmpty()) {
        continue;
      }

      String tableName = conf.getPathToPartitionInfo().get(e.getKey()).getTableName();
      if (tableNameToConf.containsKey(tableName)) {
        continue;
      }
      for (String alias: aliases) {
        Operator<?> rootOp = conf.getAliasToWork().get(alias);
        if (!(rootOp instanceof TableScanOperator)) {
          continue;
        }
        TableScanDesc tableScanDesc = ((TableScanOperator) rootOp).getConf();
        List<String> nestedColumnPaths = tableScanDesc.getNeededNestedColumnPaths();
        if (nestedColumnPaths == null || nestedColumnPaths.isEmpty()) {
          continue;
        }
        if (!tableNameToConf.containsKey(tableName)) {
          Configuration clonedConf = new Configuration(hconf);
          clonedConf.unset(ColumnProjectionUtils.READ_NESTED_COLUMN_PATH_CONF_STR);
          clonedConf.unset(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
          clonedConf.unset(ColumnProjectionUtils.FETCH_VIRTUAL_COLUMNS_CONF_STR);
          clonedConf.unset(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
          tableNameToConf.put(tableName, clonedConf);
        }
        Configuration newConf = tableNameToConf.get(tableName);
        ColumnProjectionUtils.appendReadColumns(newConf, tableScanDesc.getNeededColumnIDs(),
            tableScanDesc.getOutputColumnNames(), tableScanDesc.getNeededNestedColumnPaths(),
            tableScanDesc.hasVirtualCols());
      }
    }

    // Assign tables without nested column pruning info to the default conf
    for (PartitionDesc pd : conf.getPathToPartitionInfo().values()) {
      if (!tableNameToConf.containsKey(pd.getTableName())) {
        tableNameToConf.put(pd.getTableName(), hconf);
      }
    }

    for (PartitionDesc pd: conf.getAliasToPartnInfo().values()) {
      if (!tableNameToConf.containsKey(pd.getTableName())) {
        tableNameToConf.put(pd.getTableName(), hconf);
      }
    }

    return tableNameToConf;
  }

  /*
   * This is the same as the setChildren method below but for empty tables.
   * It takes care of the following:
   * 1. Create the right object inspector.
   * 2. Set up the childrenOpToOI with the object inspector.
   * So as to ensure that the initialization happens correctly.
   */
  public void initEmptyInputChildren(List<Operator<?>> children, Configuration hconf)
    throws SerDeException, Exception {
    setChildOperators(children);

    Map<String, Configuration> tableNameToConf = cloneConfsForColPruning(hconf);

    for (Operator<?> child : children) {
      TableScanOperator tsOp = (TableScanOperator) child;
      StructObjectInspector soi = null;
      PartitionDesc partDesc = conf.getAliasToPartnInfo().get(tsOp.getConf().getAlias());
      Configuration newConf = tableNameToConf.get(partDesc.getTableDesc().getTableName());
      AbstractSerDe serde = partDesc.getTableDesc().getSerDe();
      partDesc.setProperties(partDesc.getProperties());
      MapOpCtx opCtx = new MapOpCtx(tsOp.getConf().getAlias(), child, partDesc);
      StructObjectInspector tableRowOI = (StructObjectInspector) serde.getObjectInspector();
      initObjectInspector(newConf, opCtx, tableRowOI);
      soi = opCtx.rowObjectInspector;
      child.getParentOperators().add(this);
      childrenOpToOI.put(child, soi);
    }
  }

  public void setChildren(Configuration hconf) throws Exception {

    List<Operator<? extends OperatorDesc>> children =
        new ArrayList<Operator<? extends OperatorDesc>>();

    Map<String, Configuration> tableNameToConf = cloneConfsForColPruning(hconf);
    Map<TableDesc, StructObjectInspector> convertedOI = getConvertedOI(tableNameToConf);

    for (Map.Entry<Path, List<String>> entry : conf.getPathToAliases().entrySet()) {
      Path onefile = entry.getKey();
      List<String> aliases = entry.getValue();
      PartitionDesc partDesc = conf.getPathToPartitionInfo().get(onefile);
      TableDesc tableDesc = partDesc.getTableDesc();
      Configuration newConf = tableNameToConf.get(tableDesc.getFullTableName());

      for (String alias : aliases) {
        Operator<? extends OperatorDesc> op = conf.getAliasToWork().get(alias);
        LOG.debug("Adding alias {} to work list for file {}", alias, onefile);
        Map<Operator<?>, MapOpCtx> contexts = opCtxMap.computeIfAbsent(onefile,
                k -> new LinkedHashMap<>());
        if (contexts.containsKey(op)) {
          continue;
        }
        MapOpCtx context = new MapOpCtx(alias, op, partDesc);
        StructObjectInspector tableRowOI = convertedOI.get(partDesc.getTableDesc());
        contexts.put(op, initObjectInspector(newConf, context, tableRowOI));

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
        if (LOG.isDebugEnabled()) {
          LOG.debug("dump " + context.op + " " + context.rowObjectInspector.getTypeName());
        }
      }
    }
  }

  /** Kryo ctor. */
  protected MapOperator() {
    super();
  }

  public MapOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  public void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
  }

  public void initializeMapOperator(Configuration hconf) throws HiveException {
    super.initializeMapOperator(hconf);

    cntr = 1;
    logEveryNRows = HiveConf.getLongVar(hconf, HiveConf.ConfVars.HIVE_LOG_N_RECORDS);

    for (Entry<Operator<?>, StructObjectInspector> entry : childrenOpToOI.entrySet()) {
      Operator<?> child = entry.getKey();
      child.initialize(hconf, new ObjectInspector[] {entry.getValue()});
    }
  }

  // Find context for current input file
  @Override
  public void cleanUpInputFileChangedOp() throws HiveException {
    super.cleanUpInputFileChangedOp();
    Path fpath = getExecContext().getCurrentInputPath();
    Path nominalPath = getNominalPath(fpath);
    Map<Operator<?>, MapOpCtx> contexts = opCtxMap.get(nominalPath);
    if (LOG.isInfoEnabled()) {
      StringBuilder builder = new StringBuilder();
      for (MapOpCtx context : contexts.values()) {
        if (builder.length() > 0) {
          builder.append(", ");
        }
        builder.append(context.alias);
      }
      LOG.debug("Processing alias(es) {} for file {}", builder, fpath);
    }
    // Add alias, table name, and partitions to hadoop conf so that their
    // children will inherit these
    for (Entry<Operator<?>, MapOpCtx> entry : contexts.entrySet()) {
      Operator<?> operator = entry.getKey();
      MapOpCtx context = entry.getValue();
      operator.setInputContext(context.tableName, context.partName);
    }
    currentCtxs = contexts.values().toArray(new MapOpCtx[contexts.size()]);
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
        String message = null;
        try {
          message = toErrorMessage(value, row, current.rowObjectInspector);
        } catch (Throwable t) {
          message = "[" + row + ", " + value + "]: cannot get error message " + t.getMessage();
        }
        if (row == null) {
          deserialize_error_count.set(deserialize_error_count.get() + 1);
          LOG.trace("Hive Runtime Error while processing writable " + message);
          throw new HiveException("Hive Runtime Error while processing writable", e);
        }

        // Log the contents of the row that caused exception so that it's available for debugging. But
        // when exposed through an error message it can leak sensitive information, even to the
        // client application.
        LOG.trace("Hive Runtime Error while processing row " + message);
        throw new HiveException("Hive Runtime Error while processing row", e);
      }
    }
    rowsForwarded(childrenDone, 1);
  }

  protected final void rowsForwarded(int childrenDone, int rows) {
    numRows += rows;
    if (LOG.isInfoEnabled()) {
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
      switch(vcs.get(i)) {
        case FILENAME :
          if (ctx.inputFileChanged()) {
            vcValues[i] = new Text(ctx.getCurrentInputPath().toString());
          }
          break;
        case BLOCKOFFSET: {
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
        }
        break;
        case ROWOFFSET: {
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
        }
        break;
        case RAWDATASIZE:
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
          break;
        case ROWID:
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
          break;
        case PARTITION_SPEC_ID:
          vcValues[i] = null;
          if (ctx.getIoCxt().getPositionDeleteInfo() != null) {
            vcValues[i] = new IntWritable(ctx.getIoCxt().getPositionDeleteInfo().getSpecId());
          }
          break;
        case PARTITION_HASH:
          vcValues[i] = null;
          if (ctx.getIoCxt().getPositionDeleteInfo() != null) {
            vcValues[i] = new LongWritable(ctx.getIoCxt().getPositionDeleteInfo().getPartitionHash());
          }
          break;
        case PARTITION_PROJECTION:
          vcValues[i] = null;
          if (ctx.getIoCxt().getPositionDeleteInfo() != null) {
            vcValues[i] = new Text(ctx.getIoCxt().getPositionDeleteInfo().getPartitionProjection());
          }
          break;
        case FILE_PATH:
          vcValues[i] = null;
          if (ctx.getIoCxt().getPositionDeleteInfo() != null) {
            vcValues[i] = new Text(ctx.getIoCxt().getPositionDeleteInfo().getFilePath());
          }
          break;
        case ROW_POSITION:
          vcValues[i] = null;
          if (ctx.getIoCxt().getPositionDeleteInfo() != null) {
            vcValues[i] = new LongWritable(ctx.getIoCxt().getPositionDeleteInfo().getFilePos());
          }
          break;
        case ROWISDELETED:
          vcValues[i] = new BooleanWritable(ctx.getIoCxt().isDeletedRecord());
          break;
      }
    }
    return vcValues;
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    super.closeOp(abort);
    LOG.info("{}: Total records read - {}. abort - {}", this, numRows, abort);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    throw new HiveException("Hive 2 Internal error: should not be called!");
  }

  @Override
  public String getName() {
    return MapOperator.getOperatorName();
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
    Path nominalPath = getNominalPath(fpath);
    Map<Operator<?>, MapOpCtx> contexts = opCtxMap.get(nominalPath);
    currentCtxs = contexts.values().toArray(new MapOpCtx[contexts.size()]);
  }

  public Deserializer getCurrentDeserializer() {

    return currentCtxs[0].deserializer;
  }
}
