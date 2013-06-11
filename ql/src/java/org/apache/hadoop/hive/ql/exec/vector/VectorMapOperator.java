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

package org.apache.hadoop.hive.ql.exec.vector;

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
import org.apache.hadoop.hive.ql.exec.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Map operator. This triggers overall map side processing. This is a little
 * different from regular operators in that it starts off by processing a
 * Writable data structure from a Table (instead of a Hive Object).
 **/
public class VectorMapOperator extends Operator<MapredWork> implements Serializable, Cloneable {

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
  private transient StructObjectInspector tblRowObjectInspector;
  // convert from partition to table schema
  private transient Converter partTblObjectInspectorConverter;
  private transient boolean isPartitioned;
  private Map<MapInputPath, MapOpCtx> opCtxMap;
  private final Set<MapInputPath> listInputPaths = new HashSet<MapInputPath>();

  private Map<Operator<? extends OperatorDesc>, ArrayList<String>> operatorToPaths;

  private final Map<Operator<? extends OperatorDesc>, MapOpCtx> childrenOpToOpCtxMap =
    new HashMap<Operator<? extends OperatorDesc>, MapOpCtx>();

  private ArrayList<Operator<? extends OperatorDesc>> extraChildrenToClose = null;
  private VectorizationContext vectorizationContext = null;
  private boolean outputColumnsInitialized = false;;

  private static class MapInputPath {
    String path;
    String alias;
    Operator<? extends OperatorDesc> op;

    /**
     * @param path
     * @param alias
     * @param op
     */
    public MapInputPath(String path, String alias,
        Operator<? extends OperatorDesc> op) {
      this.path = path;
      this.alias = alias;
      this.op = op;
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

    public Operator<? extends OperatorDesc> getOp() {
      return op;
    }

    public void setOp(Operator<? extends OperatorDesc> op) {
      this.op = op;
    }

  }

  private static class MapOpCtx {
    private final boolean isPartitioned;
    private final StructObjectInspector tblRawRowObjectInspector; // without partition
    private final StructObjectInspector partObjectInspector; // partition
    private final StructObjectInspector rowObjectInspector;
    private final Converter partTblObjectInspectorConverter;
    private final Object[] rowWithPart;
    private final Object[] rowWithPartAndVC;
    private final Deserializer deserializer;
    private String tableName;
    private String partName;

    /**
     * @param isPartitioned
     * @param rowObjectInspector
     * @param rowWithPart
     */
    public MapOpCtx(boolean isPartitioned,
        StructObjectInspector rowObjectInspector,
        StructObjectInspector tblRawRowObjectInspector,
        StructObjectInspector partObjectInspector,
        Object[] rowWithPart,
        Object[] rowWithPartAndVC,
        Deserializer deserializer,
        Converter partTblObjectInspectorConverter) {
      this.isPartitioned = isPartitioned;
      this.rowObjectInspector = rowObjectInspector;
      this.tblRawRowObjectInspector = tblRawRowObjectInspector;
      this.partObjectInspector = partObjectInspector;
      this.rowWithPart = rowWithPart;
      this.rowWithPartAndVC = rowWithPartAndVC;
      this.deserializer = deserializer;
      this.partTblObjectInspectorConverter = partTblObjectInspectorConverter;
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

    public StructObjectInspector getTblRawRowObjectInspector() {
      return tblRawRowObjectInspector;
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

    public Converter getPartTblObjectInspectorConverter() {
      return partTblObjectInspectorConverter;
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
      Configuration hconf, String onefile, Map<TableDesc, StructObjectInspector> convertedOI)
          throws HiveException,
      ClassNotFoundException, InstantiationException, IllegalAccessException,
      SerDeException {
    PartitionDesc pd = conf.getPathToPartitionInfo().get(onefile);
    LinkedHashMap<String, String> partSpec = pd.getPartSpec();
    // Use tblProps in case of unpartitioned tables
    Properties partProps =
        (pd.getPartSpec() == null || pd.getPartSpec().isEmpty()) ?
            pd.getTableDesc().getProperties() : pd.getProperties();

    Class serdeclass = pd.getDeserializerClass();
    if (serdeclass == null) {
      String className = pd.getSerdeClassName();
      if ((className == null) || (className.isEmpty())) {
        throw new HiveException(
            "SerDe class or the SerDe class name is not set for table: "
                + pd.getProperties().getProperty("name"));
      }
      serdeclass = hconf.getClassByName(className);
    }

    String tableName = String.valueOf(partProps.getProperty("name"));
    String partName = String.valueOf(partSpec);
    Deserializer partDeserializer = (Deserializer) serdeclass.newInstance();
    partDeserializer.initialize(hconf, partProps);
    StructObjectInspector partRawRowObjectInspector = (StructObjectInspector) partDeserializer
        .getObjectInspector();

    StructObjectInspector tblRawRowObjectInspector = convertedOI.get(pd.getTableDesc());

    partTblObjectInspectorConverter =
    ObjectInspectorConverters.getConverter(partRawRowObjectInspector,
        tblRawRowObjectInspector);

    MapOpCtx opCtx = null;
    // Next check if this table has partitions and if so
    // get the list of partition names as well as allocate
    // the serdes for the partition columns
    String pcols = partProps
        .getProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
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
          // for partitionless table, initialize partValue to null
          partValues[i] = null;
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
              .asList(new StructObjectInspector[] {tblRawRowObjectInspector, partObjectInspector}));
      // LOG.info("dump " + tableName + " " + partName + " " +
      // rowObjectInspector.getTypeName());
      opCtx = new MapOpCtx(true, rowObjectInspector, tblRawRowObjectInspector, partObjectInspector,
                           rowWithPart, null, partDeserializer, partTblObjectInspectorConverter);
    } else {
      // LOG.info("dump2 " + tableName + " " + partName + " " +
      // rowObjectInspector.getTypeName());
      opCtx = new MapOpCtx(false, tblRawRowObjectInspector, tblRawRowObjectInspector, null, null,
                           null, partDeserializer, partTblObjectInspectorConverter);
    }
    opCtx.tableName = tableName;
    opCtx.partName = partName;
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
    try
    {
      for (String onefile : conf.getPathToAliases().keySet()) {
        PartitionDesc pd = conf.getPathToPartitionInfo().get(onefile);
        TableDesc tableDesc = pd.getTableDesc();
        Properties tblProps = tableDesc.getProperties();
        // If the partition does not exist, use table properties
        Properties partProps =
            (pd.getPartSpec() == null || pd.getPartSpec().isEmpty()) ?
                tblProps : pd.getProperties();

        Class sdclass = pd.getDeserializerClass();
        if (sdclass == null) {
          String className = pd.getSerdeClassName();
          if ((className == null) || (className.isEmpty())) {
            throw new HiveException(
                "SerDe class or the SerDe class name is not set for table: "
                    + pd.getProperties().getProperty("name"));
          }
          sdclass = hconf.getClassByName(className);
        }

        Deserializer partDeserializer = (Deserializer) sdclass.newInstance();
        partDeserializer.initialize(hconf, partProps);
        StructObjectInspector partRawRowObjectInspector = (StructObjectInspector) partDeserializer
            .getObjectInspector();

        StructObjectInspector tblRawRowObjectInspector = tableDescOI.get(tableDesc);
        if ((tblRawRowObjectInspector == null) ||
            (identityConverterTableDesc.contains(tableDesc))) {
          sdclass = tableDesc.getDeserializerClass();
          if (sdclass == null) {
            String className = tableDesc.getSerdeClassName();
            if ((className == null) || (className.isEmpty())) {
              throw new HiveException(
                  "SerDe class or the SerDe class name is not set for table: "
                      + tableDesc.getProperties().getProperty("name"));
            }
            sdclass = hconf.getClassByName(className);
          }
          Deserializer tblDeserializer = (Deserializer) sdclass.newInstance();
          tblDeserializer.initialize(hconf, tblProps);
          tblRawRowObjectInspector =
              (StructObjectInspector) ObjectInspectorConverters.getConvertedOI(
                  partRawRowObjectInspector,
                  (StructObjectInspector) tblDeserializer.getObjectInspector());

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

  public void setChildren(Configuration hconf) throws HiveException {

    Path fpath = new Path((new Path(HiveConf.getVar(hconf,
        HiveConf.ConfVars.HADOOPMAPFILENAME))).toUri().getPath());

    ArrayList<Operator<? extends OperatorDesc>> children =
      new ArrayList<Operator<? extends OperatorDesc>>();
    opCtxMap = new HashMap<MapInputPath, MapOpCtx>();
    operatorToPaths = new HashMap<Operator<? extends OperatorDesc>, ArrayList<String>>();

    statsMap.put(Counter.DESERIALIZE_ERRORS, deserialize_error_count);
    Map<TableDesc, StructObjectInspector> convertedOI = getConvertedOI(hconf);
    Map<String, Operator<? extends OperatorDesc>> aliasToVectorOpMap =
        new HashMap<String, Operator<? extends OperatorDesc>>();

    try {
      for (String onefile : conf.getPathToAliases().keySet()) {
        MapOpCtx opCtx = initObjectInspector(conf, hconf, onefile, convertedOI);
        //Create columnMap
        Map<String, Integer> columnMap = new HashMap<String, Integer>();
        StructObjectInspector rowInspector = opCtx.getRowObjectInspector();

        int columnCount = 0;
        for (StructField sfield : rowInspector.getAllStructFieldRefs()) {
          columnMap.put(sfield.getFieldName(), columnCount);
          System.out.println("Column Name: " + sfield.getFieldName() + ", " +
              "column index: " + columnCount);
          LOG.info("Column Name: " + sfield.getFieldName() + ", " +
              "column index: " + columnCount);
          columnCount++;
        }

        Path onepath = new Path(new Path(onefile).toUri().getPath());
        List<String> aliases = conf.getPathToAliases().get(onefile);

        vectorizationContext  = new VectorizationContext(columnMap, columnCount);

        for (String onealias : aliases) {
          Operator<? extends OperatorDesc> op = conf.getAliasToWork().get(
            onealias);
          LOG.info("Adding alias " + onealias + " to work list for file "
            + onefile);

          Operator<? extends OperatorDesc> vectorOp = aliasToVectorOpMap.get(onealias);

          if (vectorOp == null) {
            vectorOp = vectorizeOperator(op, vectorizationContext);
            aliasToVectorOpMap.put(onealias, vectorOp);
          }

          System.out.println("Using vectorized op: "+ vectorOp.getName());
          LOG.info("Using vectorized op: " + vectorOp.getName());
          op = vectorOp;
          MapInputPath inp = new MapInputPath(onefile, onealias, op);
          opCtxMap.put(inp, opCtx);
          if (operatorToPaths.get(op) == null) {
            operatorToPaths.put(op, new ArrayList<String>());
          }
          operatorToPaths.get(op).add(onefile);
          op.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>());
          op.getParentOperators().add(this);
          // check for the operators who will process rows coming to this Map
          // Operator
          if (!onepath.toUri().relativize(fpath.toUri()).equals(fpath.toUri())) {
            children.add(op);
            childrenOpToOpCtxMap.put(op, opCtx);
            LOG.info("dump " + op.getName() + " "
                + opCtxMap.get(inp).getRowObjectInspector().getTypeName());
          }
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

  public static Operator<? extends OperatorDesc> vectorizeOperator
      (Operator<? extends OperatorDesc> op, VectorizationContext
          vectorizationContext) throws HiveException, CloneNotSupportedException {

    Operator<? extends OperatorDesc> vectorOp;
    boolean recursive = true;

    switch (op.getType()) {
      case GROUPBY:
        vectorOp = new VectorGroupByOperator(vectorizationContext, op.getConf());
        recursive = false;
        break;
      case FILTER:
        vectorOp = new VectorFilterOperator(vectorizationContext, op.getConf());
        break;
      case SELECT:
        vectorOp = new VectorSelectOperator(vectorizationContext, op.getConf());
        break;
      case FILESINK:
        vectorOp = new VectorFileSinkOperator(vectorizationContext, op.getConf());
        break;
      case TABLESCAN:
        vectorOp = op.cloneOp();
        break;
      case REDUCESINK:
        vectorOp = new VectorReduceSinkOperator(vectorizationContext, op.getConf());
        break;
      default:
        throw new HiveException("Operator: " + op.getName() + ", " +
            "not vectorized");
    }

    if (recursive) {
      List<Operator<? extends OperatorDesc>> children = op.getChildOperators();
      if (children != null && !children.isEmpty()) {
        List<Operator<? extends OperatorDesc>> vectorizedChildren = new
            ArrayList<Operator<? extends OperatorDesc>>(children.size());
        for (Operator<? extends OperatorDesc> childOp : children) {
          Operator<? extends OperatorDesc> vectorizedChild =
              vectorizeOperator(childOp, vectorizationContext);
          List<Operator<? extends OperatorDesc>> parentList =
              new ArrayList<Operator<? extends OperatorDesc>>();
          parentList.add(vectorOp);
          vectorizedChild.setParentOperators(parentList);
          vectorizedChildren.add(vectorizedChild);
        }
        vectorOp.setChildOperators(vectorizedChildren);
      }
    } else {
      // transfer the row-mode children to the vectorized op parent
      List<Operator<? extends OperatorDesc>> children =
          new ArrayList<Operator<? extends OperatorDesc>>();

      if (op.getChildOperators() != null && !op.getChildOperators().isEmpty()) {
        List<Operator<? extends OperatorDesc>> parentList =
            new ArrayList<Operator<? extends OperatorDesc>>();
        parentList.add(vectorOp);
        for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
          Operator<? extends OperatorDesc> clonedOp = childOp.cloneRecursiveChildren();
          clonedOp.setParentOperators(parentList);
          children.add(clonedOp);
        }
        vectorOp.setChildOperators(children);
      }
    }

    return vectorOp;
  }

  private void plugIntermediate (Operator<? extends OperatorDesc> parent,
      Operator<? extends OperatorDesc> plug) {

    List<Operator<? extends OperatorDesc>> plugList =
        new ArrayList<Operator<? extends OperatorDesc>>();
    plugList.add(plug);

    List<Operator<? extends OperatorDesc>> parentAsList =
        new ArrayList<Operator<? extends OperatorDesc>>();
    parentAsList.add(parent);


    List<Operator<? extends OperatorDesc>> children = parent.getChildOperators();
    if (children != null && !children.isEmpty()) {
      for (Operator<? extends OperatorDesc> childOp : children) {
        childOp.setParentOperators(plugList);
      }
    }
    plug.setChildOperators(children);
    plug.setParentOperators(parentAsList);
    parent.setChildOperators (plugList);
  }

  @Override
  public void initializeOp(Configuration hconf) throws HiveException {
    // set that parent initialization is done and call initialize on children
    state = State.INIT;
    List<Operator<? extends OperatorDesc>> children = getChildOperators();

    for (Entry<Operator<? extends OperatorDesc>, MapOpCtx> entry : childrenOpToOpCtxMap
        .entrySet()) {
      Operator<? extends OperatorDesc> child = entry.getKey();
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
      Operator<? extends OperatorDesc> op = input.op;
      // op is not in the children list, so need to remember it and close it
      // afterwards
      if (children.indexOf(op) == -1) {
        if (extraChildrenToClose == null) {
          extraChildrenToClose = new ArrayList<Operator<? extends OperatorDesc>>();
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
      for (Operator<? extends OperatorDesc> op : extraChildrenToClose) {
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
        Operator<? extends OperatorDesc> op =
            conf.getAliasToWork().get(onealias);

        LOG.info("Processing alias " + onealias + " for file " + onefile);

        MapInputPath inp = new MapInputPath(onefile, onealias, op);
        //setInspectorInput(inp);
        break;
      }
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

  public void process(Object value) throws HiveException {
    // A mapper can span multiple files/partitions.
    // The serializers need to be reset if the input file changed
    if ((this.getExecContext() != null) &&
        this.getExecContext().inputFileChanged()) {
      // The child operators cleanup if input file has changed
      cleanUpInputFileChanged();
    }

    // The row has been converted to comply with table schema, irrespective of partition schema.
    // So, use tblOI (and not partOI) for forwarding
    try {
      if (value instanceof VectorizedRowBatch) {
        if (!outputColumnsInitialized ) {
          VectorizedRowBatch vrg = (VectorizedRowBatch) value;
          Map<Integer, String> outputColumnTypes =
              vectorizationContext.getOutputColumnTypeMap();
          if (!outputColumnTypes.isEmpty()) {
            int origNumCols = vrg.numCols;
            int newNumCols = vrg.cols.length+outputColumnTypes.keySet().size();
            vrg.cols = Arrays.copyOf(vrg.cols, newNumCols);
            for (int i = origNumCols; i < newNumCols; i++) {
              vrg.cols[i] = vectorizationContext.allocateColumnVector(outputColumnTypes.get(i),
                  VectorizedRowBatch.DEFAULT_SIZE);
            }
          }
          outputColumnsInitialized = true;
        }
        forward(value, null);
      } else {
        Object row = null;
        row = this.partTblObjectInspectorConverter.convert(deserializer.deserialize((Writable) value));
        forward(row, tblRowObjectInspector);
      }
    } catch (Exception e) {
      throw new HiveException("Hive Runtime Error while processing ", e);
    }
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
