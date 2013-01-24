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
public class MapOperator extends Operator<MapredWork> implements Serializable, Cloneable {

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
  private transient boolean hasVC;
  private Map<MapInputPath, MapOpCtx> opCtxMap;
  private final Set<MapInputPath> listInputPaths = new HashSet<MapInputPath>();

  private Map<Operator<? extends OperatorDesc>, ArrayList<String>> operatorToPaths;

  private final Map<Operator<? extends OperatorDesc>, MapOpCtx> childrenOpToOpCtxMap =
    new HashMap<Operator<? extends OperatorDesc>, MapOpCtx>();

  private ArrayList<Operator<? extends OperatorDesc>> extraChildrenToClose = null;

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
    private StructObjectInspector rowObjectInspector;
    private final Converter partTblObjectInspectorConverter;
    private final Object[] rowWithPart;
    private Object[] rowWithPartAndVC;
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

  /**
   * Set the inspectors given a input. Since a mapper can span multiple partitions, the inspectors
   * need to be changed if the input changes
   **/
  private void setInspectorInput(MapInputPath inp) {
    Operator<? extends OperatorDesc> op = inp.getOp();

    deserializer = opCtxMap.get(inp).getDeserializer();
    isPartitioned = opCtxMap.get(inp).isPartitioned();
    rowWithPart = opCtxMap.get(inp).getRowWithPart();
    rowWithPartAndVC = opCtxMap.get(inp).getRowWithPartAndVC();
    tblRowObjectInspector = opCtxMap.get(inp).getRowObjectInspector();
    partTblObjectInspectorConverter = opCtxMap.get(inp).getPartTblObjectInspectorConverter();
    if (listInputPaths.contains(inp)) {
      return;
    }

    listInputPaths.add(inp);

    // The op may not be a TableScan for mapjoins
    // Consider the query: select /*+MAPJOIN(a)*/ count(*) FROM T1 a JOIN T2 b ON a.key = b.key;
    // In that case, it will be a Select, but the rowOI need not be ammended
    if (op instanceof TableScanOperator) {
      StructObjectInspector tblRawRowObjectInspector =
          opCtxMap.get(inp).getTblRawRowObjectInspector();
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
            this.tblRowObjectInspector = ObjectInspectorFactory.getUnionStructObjectInspector(Arrays
                                        .asList(new StructObjectInspector[] {
                                            tblRowObjectInspector, vcStructObjectInspector}));
          } else {
            this.tblRowObjectInspector = ObjectInspectorFactory.getUnionStructObjectInspector(Arrays
                                        .asList(new StructObjectInspector[] {
                                            tblRawRowObjectInspector, partObjectInspector,
                                            vcStructObjectInspector}));
          }
          opCtxMap.get(inp).rowObjectInspector = this.tblRowObjectInspector;
          opCtxMap.get(inp).rowWithPartAndVC = this.rowWithPartAndVC;
        }
      }
    }
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
    try {
      for (String onefile : conf.getPathToAliases().keySet()) {
        MapOpCtx opCtx = initObjectInspector(conf, hconf, onefile, convertedOI);
        Path onepath = new Path(new Path(onefile).toUri().getPath());
        List<String> aliases = conf.getPathToAliases().get(onefile);

        for (String onealias : aliases) {
          Operator<? extends OperatorDesc> op = conf.getAliasToWork().get(
            onealias);
          LOG.info("Adding alias " + onealias + " to work list for file "
            + onefile);
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
        this.rowWithPartAndVC[0] =
            partTblObjectInspectorConverter.convert(deserializer.deserialize(value));
        int vcPos = isPartitioned ? 2 : 1;
        if (context != null) {
          populateVirtualColumnValues(context, vcs, vcValues, deserializer);
        }
        this.rowWithPartAndVC[vcPos] = this.vcValues;
      } else if (!isPartitioned) {
        row = partTblObjectInspectorConverter.convert(deserializer.deserialize((Writable) value));
      } else {
        rowWithPart[0] =
            partTblObjectInspectorConverter.convert(deserializer.deserialize((Writable) value));
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
      if (this.hasVC) {
        forward(this.rowWithPartAndVC, this.tblRowObjectInspector);
      } else if (!isPartitioned) {
        forward(row, tblRowObjectInspector);
      } else {
        forward(rowWithPart, tblRowObjectInspector);
      }
    } catch (Exception e) {
      // Serialize the row and output the error message.
      String rowString;
      try {
        if (this.hasVC) {
          rowString = SerDeUtils.getJSONString(rowWithPartAndVC, tblRowObjectInspector);
        } else if (!isPartitioned) {
          rowString = SerDeUtils.getJSONString(row, tblRowObjectInspector);
        } else {
          rowString = SerDeUtils.getJSONString(rowWithPart, tblRowObjectInspector);
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
