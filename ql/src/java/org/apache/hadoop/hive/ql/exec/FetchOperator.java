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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringEscapeUtils;
import com.google.common.collect.Iterators;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveContextAwareRecordReader;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveRecordReader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.AnnotationUtils;

/**
 * FetchTask implementation.
 **/
public class FetchOperator implements Serializable {

  static final Log LOG = LogFactory.getLog(FetchOperator.class.getName());
  static final LogHelper console = new LogHelper(LOG);

  public static final String FETCH_OPERATOR_DIRECTORY_LIST =
      "hive.complete.dir.list";

  private FetchWork work;
  private Operator<?> operator;    // operator tree for processing row further (optional)

  private final boolean hasVC;
  private final boolean isStatReader;
  private final boolean isPartitioned;
  private final boolean isNonNativeTable;
  private StructObjectInspector vcsOI;
  private List<VirtualColumn> vcCols;
  private ExecMapperContext context;

  private transient Deserializer tableSerDe;
  private transient StructObjectInspector tableOI;
  private transient StructObjectInspector partKeyOI;
  private transient StructObjectInspector convertedOI;

  private transient Iterator<Path> iterPath;
  private transient Iterator<PartitionDesc> iterPartDesc;
  private transient Iterator<FetchInputFormatSplit> iterSplits = Iterators.emptyIterator();

  private transient Path currPath;
  private transient PartitionDesc currDesc;
  private transient Deserializer currSerDe;
  private transient Converter ObjectConverter;
  private transient RecordReader<WritableComparable, Writable> currRecReader;

  private transient JobConf job;
  private transient WritableComparable key;
  private transient Writable value;
  private transient Object[] vcValues;

  private transient int headerCount;
  private transient int footerCount;
  private transient FooterBuffer footerBuffer;

  private transient StructObjectInspector outputOI;
  private transient Object[] row;

  public FetchOperator(FetchWork work, JobConf job) throws HiveException {
    this(work, job, null, null);
  }

  public FetchOperator(FetchWork work, JobConf job, Operator<?> operator,
      List<VirtualColumn> vcCols) throws HiveException {
    this.job = job;
    this.work = work;
    this.operator = operator;
    this.vcCols = vcCols;
    this.hasVC = vcCols != null && !vcCols.isEmpty();
    this.isStatReader = work.getTblDesc() == null;
    this.isPartitioned = !isStatReader && work.isPartitioned();
    this.isNonNativeTable = !isStatReader && work.getTblDesc().isNonNative();
    initialize();
  }

  private void initialize() throws HiveException {
    if (isStatReader) {
      outputOI = work.getStatRowOI();
      return;
    }
    if (hasVC) {
      List<String> names = new ArrayList<String>(vcCols.size());
      List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(vcCols.size());
      for (VirtualColumn vc : vcCols) {
        inspectors.add(vc.getObjectInspector());
        names.add(vc.getName());
      }
      vcsOI = ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
      vcValues = new Object[vcCols.size()];
    }
    if (hasVC && isPartitioned) {
      row = new Object[3];
    } else if (hasVC || isPartitioned) {
      row = new Object[2];
    } else {
      row = new Object[1];
    }
    if (isPartitioned) {
      iterPath = work.getPartDir().iterator();
      iterPartDesc = work.getPartDesc().iterator();
    } else {
      iterPath = Arrays.asList(work.getTblDir()).iterator();
      iterPartDesc = Iterators.cycle(new PartitionDesc(work.getTblDesc(), null));
    }
    outputOI = setupOutputObjectInspector();
    context = setupExecContext(operator, work.getPathLists());
  }

  private ExecMapperContext setupExecContext(Operator operator, List<Path> paths) {
    ExecMapperContext context = null;
    if (hasVC || work.getSplitSample() != null) {
      context = new ExecMapperContext(job);
      if (operator != null) {
        operator.setExecContext(context);
      }
    }
    setFetchOperatorContext(job, paths);
    return context;
  }

  public FetchWork getWork() {
    return work;
  }

  public void setWork(FetchWork work) {
    this.work = work;
  }

  /**
   * A cache of InputFormat instances.
   */
  private static final Map<String, InputFormat> inputFormats = new HashMap<String, InputFormat>();

  @SuppressWarnings("unchecked")
  static InputFormat getInputFormatFromCache(Class<? extends InputFormat> inputFormatClass,
       JobConf conf) throws IOException {
    if (Configurable.class.isAssignableFrom(inputFormatClass) ||
        JobConfigurable.class.isAssignableFrom(inputFormatClass)) {
      return (InputFormat<WritableComparable, Writable>) ReflectionUtils
          .newInstance(inputFormatClass, conf);
    }
    InputFormat format = inputFormats.get(inputFormatClass.getName());
    if (format == null) {
      try {
        format = ReflectionUtils.newInstance(inputFormatClass, conf);
        inputFormats.put(inputFormatClass.getName(), format);
      } catch (Exception e) {
        throw new IOException("Cannot create an instance of InputFormat class "
            + inputFormatClass.getName() + " as specified in mapredWork!", e);
      }
    }
    return format;
  }

  private StructObjectInspector getPartitionKeyOI(TableDesc tableDesc) throws Exception {
    String pcols = tableDesc.getProperties().getProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    String pcolTypes = tableDesc.getProperties().getProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);

    String[] partKeys = pcols.trim().split("/");
    String[] partKeyTypes = pcolTypes.trim().split(":");
    ObjectInspector[] inspectors = new ObjectInspector[partKeys.length];
    for (int i = 0; i < partKeys.length; i++) {
      inspectors[i] = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
          TypeInfoFactory.getPrimitiveTypeInfo(partKeyTypes[i]));
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        Arrays.asList(partKeys), Arrays.asList(inspectors));
  }

  private Object[] createPartValue(PartitionDesc partDesc, StructObjectInspector partOI) {
    Map<String, String> partSpec = partDesc.getPartSpec();
    List<? extends StructField> fields = partOI.getAllStructFieldRefs();
    Object[] partValues = new Object[fields.size()];
    for (int i = 0; i < partValues.length; i++) {
      StructField field = fields.get(i);
      String value = partSpec.get(field.getFieldName());
      ObjectInspector oi = field.getFieldObjectInspector();
      partValues[i] = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi).convert(value);
    }
    return partValues;
  }

  private boolean getNextPath() throws Exception {
    while (iterPath.hasNext()) {
      currPath = iterPath.next();
      currDesc = iterPartDesc.next();
      if (isNonNativeTable) {
        return true;
      }
      FileSystem fs = currPath.getFileSystem(job);
      if (fs.exists(currPath)) {
        for (FileStatus fStat : listStatusUnderPath(fs, currPath)) {
          if (fStat.getLen() > 0) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Set context for this fetch operator in to the jobconf.
   * This helps InputFormats make decisions based on the scope of the complete
   * operation.
   * @param conf the configuration to modify
   * @param paths the list of input directories
   */
  static void setFetchOperatorContext(JobConf conf, List<Path> paths) {
    if (paths != null) {
      StringBuilder buff = new StringBuilder();
      for (Path path : paths) {
        if (buff.length() > 0) {
          buff.append('\t');
        }
        buff.append(StringEscapeUtils.escapeJava(path.toString()));
      }
      conf.set(FETCH_OPERATOR_DIRECTORY_LIST, buff.toString());
    }
  }

  private RecordReader<WritableComparable, Writable> getRecordReader() throws Exception {
    if (!iterSplits.hasNext()) {
      FetchInputFormatSplit[] splits = getNextSplits();
      if (splits == null) {
        return null;
      }
      if (!isPartitioned || convertedOI == null) {
        currSerDe = tableSerDe;
        ObjectConverter = null;
      } else {
        currSerDe = needConversion(currDesc) ? currDesc.getDeserializer(job) : tableSerDe;
        ObjectInspector inputOI = currSerDe.getObjectInspector();
        ObjectConverter = ObjectInspectorConverters.getConverter(inputOI, convertedOI);
      }
      if (isPartitioned) {
        row[1] = createPartValue(currDesc, partKeyOI);
      }
      iterSplits = Arrays.asList(splits).iterator();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating fetchTask with deserializer typeinfo: "
            + currSerDe.getObjectInspector().getTypeName());
        LOG.debug("deserializer properties:\ntable properties: " +
            currDesc.getTableDesc().getProperties() + "\npartition properties: " +
            currDesc.getProperties());
      }
    }

    final FetchInputFormatSplit target = iterSplits.next();

    @SuppressWarnings("unchecked")
    final RecordReader<WritableComparable, Writable> reader = target.getRecordReader(job);
    if (hasVC || work.getSplitSample() != null) {
      currRecReader = new HiveRecordReader<WritableComparable, Writable>(reader, job) {
        @Override
        public boolean doNext(WritableComparable key, Writable value) throws IOException {
          // if current pos is larger than shrinkedLength which is calculated for
          // each split by table sampling, stop fetching any more (early exit)
          if (target.shrinkedLength > 0 &&
              context.getIoCxt().getCurrentBlockStart() > target.shrinkedLength) {
            return false;
          }
          return super.doNext(key, value);
        }
      };
      ((HiveContextAwareRecordReader)currRecReader).
          initIOContext(target, job, target.inputFormat.getClass(), reader);
    } else {
      currRecReader = reader;
    }
    key = currRecReader.createKey();
    value = currRecReader.createValue();
    headerCount = footerCount = 0;
    return currRecReader;
  }

  protected FetchInputFormatSplit[] getNextSplits() throws Exception {
    while (getNextPath()) {
      // not using FileInputFormat.setInputPaths() here because it forces a connection to the
      // default file system - which may or may not be online during pure metadata operations
      job.set("mapred.input.dir", StringUtils.escapeString(currPath.toString()));

      // Fetch operator is not vectorized and as such turn vectorization flag off so that
      // non-vectorized record reader is created below.
      HiveConf.setBoolVar(job, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);

      Class<? extends InputFormat> formatter = currDesc.getInputFileFormatClass();
      Utilities.copyTableJobPropertiesToConf(currDesc.getTableDesc(), job);
      InputFormat inputFormat = getInputFormatFromCache(formatter, job);

      InputSplit[] splits = inputFormat.getSplits(job, 1);
      FetchInputFormatSplit[] inputSplits = new FetchInputFormatSplit[splits.length];
      for (int i = 0; i < splits.length; i++) {
        inputSplits[i] = new FetchInputFormatSplit(splits[i], inputFormat);
      }
      if (work.getSplitSample() != null) {
        inputSplits = splitSampling(work.getSplitSample(), inputSplits);
      }
      if (inputSplits.length > 0) {
        return inputSplits;
      }
    }
    return null;
  }

  private FetchInputFormatSplit[] splitSampling(SplitSample splitSample,
      FetchInputFormatSplit[] splits) {
    long totalSize = 0;
    for (FetchInputFormatSplit split: splits) {
        totalSize += split.getLength();
    }
    List<FetchInputFormatSplit> result = new ArrayList<FetchInputFormatSplit>(splits.length);
    long targetSize = splitSample.getTargetSize(totalSize);
    int startIndex = splitSample.getSeedNum() % splits.length;
    long size = 0;
    for (int i = 0; i < splits.length; i++) {
      FetchInputFormatSplit split = splits[(startIndex + i) % splits.length];
      result.add(split);
      long splitgLength = split.getLength();
      if (size + splitgLength >= targetSize) {
        if (size + splitgLength > targetSize) {
          split.shrinkedLength = targetSize - size;
        }
        break;
      }
      size += splitgLength;
    }
    return result.toArray(new FetchInputFormatSplit[result.size()]);
  }

  /**
   * Get the next row and push down it to operator tree.
   * Currently only used by FetchTask.
   **/
  public boolean pushRow() throws IOException, HiveException {
    if (work.getRowsComputedUsingStats() != null) {
      for (List<Object> row : work.getRowsComputedUsingStats()) {
        operator.processOp(row, 0);
      }
      flushRow();
      return true;
    }
    InspectableObject row = getNextRow();
    if (row != null) {
      pushRow(row);
    } else {
      flushRow();
    }
    return row != null;
  }

  protected void pushRow(InspectableObject row) throws HiveException {
    operator.processOp(row.o, 0);
  }

  protected void flushRow() throws HiveException {
    operator.flush();
  }

  private transient final InspectableObject inspectable = new InspectableObject();

  /**
   * Get the next row. The fetch context is modified appropriately.
   *
   **/
  public InspectableObject getNextRow() throws IOException {
    try {
      while (true) {
        boolean opNotEOF = true;
        if (context != null) {
          context.resetRow();
        }
        if (currRecReader == null) {
          currRecReader = getRecordReader();
          if (currRecReader == null) {
            return null;
          }

          /**
           * Start reading a new file.
           * If file contains header, skip header lines before reading the records.
           * If file contains footer, used FooterBuffer to cache and remove footer
           * records at the end of the file.
           */
          headerCount = Utilities.getHeaderCount(currDesc.getTableDesc());
          footerCount = Utilities.getFooterCount(currDesc.getTableDesc(), job);

          // Skip header lines.
          opNotEOF = Utilities.skipHeader(currRecReader, headerCount, key, value);

          // Initialize footer buffer.
          if (opNotEOF && footerCount > 0) {
            footerBuffer = new FooterBuffer();
            opNotEOF = footerBuffer.initializeBuffer(job, currRecReader, footerCount, key, value);
          }
        }

        if (opNotEOF && footerBuffer == null) {
          /**
           * When file doesn't end after skipping header line
           * and there is no footer lines, read normally.
           */
          opNotEOF = currRecReader.next(key, value);
        }
        if (opNotEOF && footerBuffer != null) {
          opNotEOF = footerBuffer.updateBuffer(job, currRecReader, key, value);
        }
        if (opNotEOF) {
          if (operator != null && context != null && context.inputFileChanged()) {
            // The child operators cleanup if input file has changed
            operator.cleanUpInputFileChanged();
          }
          if (hasVC) {
            row[isPartitioned ? 2 : 1] =
                MapOperator.populateVirtualColumnValues(context, vcCols, vcValues, currSerDe);
          }
          Object deserialized = currSerDe.deserialize(value);
          if (ObjectConverter != null) {
            deserialized = ObjectConverter.convert(deserialized);
          }

          if (hasVC || isPartitioned) {
            row[0] = deserialized;
            inspectable.o = row;
          } else {
            inspectable.o = deserialized;
          }
          inspectable.oi = currSerDe.getObjectInspector();
          return inspectable;
        } else {
          currRecReader.close();
          currRecReader = null;
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Clear the context, if anything needs to be done.
   *
   **/
  public void clearFetchContext() throws HiveException {
    try {
      if (currRecReader != null) {
        currRecReader.close();
        currRecReader = null;
      }
      if (operator != null) {
        operator.close(false);
        operator = null;
      }
      if (context != null) {
        context.clear();
        context = null;
      }
      this.currPath = null;
      this.iterPath = null;
      this.iterPartDesc = null;
      this.iterSplits = Iterators.emptyIterator();
    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + StringUtils.stringifyException(e));
    }
  }

  /**
   * used for bucket map join
   */
  public void setupContext(List<Path> paths) {
    this.iterPath = paths.iterator();
    List<PartitionDesc> partitionDescs;
    if (!isPartitioned) {
      this.iterPartDesc = Iterators.cycle(new PartitionDesc(work.getTblDesc(), null));
    } else {
      this.iterPartDesc = work.getPartDescs(paths).iterator();
    }
    this.context = setupExecContext(operator, paths);
  }

  /**
   * returns output ObjectInspector, never null
   */
  public ObjectInspector getOutputObjectInspector() {
    return outputOI;
  }

  private StructObjectInspector setupOutputObjectInspector() throws HiveException {
    TableDesc tableDesc = work.getTblDesc();
    try {
      tableSerDe = tableDesc.getDeserializer(job, true);
      tableOI = (StructObjectInspector) tableSerDe.getObjectInspector();
      if (!isPartitioned) {
        return getTableRowOI(tableOI);
      }
      partKeyOI = getPartitionKeyOI(tableDesc);

      PartitionDesc partDesc = new PartitionDesc(tableDesc, null);
      List<PartitionDesc> listParts = work.getPartDesc();
      // Chose the table descriptor if none of the partitions is present.
      // For eg: consider the query:
      // select /*+mapjoin(T1)*/ count(*) from T1 join T2 on T1.key=T2.key
      // Both T1 and T2 and partitioned tables, but T1 does not have any partitions
      // FetchOperator is invoked for T1, and listParts is empty. In that case,
      // use T1's schema to get the ObjectInspector.
      if (listParts == null || listParts.isEmpty() || !needConversion(tableDesc, listParts)) {
        return getPartitionedRowOI(tableOI);
      }
      convertedOI = (StructObjectInspector) ObjectInspectorConverters.getConvertedOI(
          tableOI, tableOI, null, false);
      return getPartitionedRowOI(convertedOI);
    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + StringUtils.stringifyException(e));
    }
  }

  private StructObjectInspector getTableRowOI(StructObjectInspector valueOI) {
    return hasVC ? ObjectInspectorFactory.getUnionStructObjectInspector(
        Arrays.asList(valueOI, vcsOI)) : valueOI;
  }

  private StructObjectInspector getPartitionedRowOI(StructObjectInspector valueOI) {
    return ObjectInspectorFactory.getUnionStructObjectInspector(
        hasVC ? Arrays.asList(valueOI, partKeyOI, vcsOI) : Arrays.asList(valueOI, partKeyOI));
  }

  private boolean needConversion(PartitionDesc partitionDesc) {
    return needConversion(partitionDesc.getTableDesc(), Arrays.asList(partitionDesc));
  }

  // if table and all partitions have the same schema and serde, no need to convert
  private boolean needConversion(TableDesc tableDesc, List<PartitionDesc> partDescs) {
    Class<?> tableSerDe = tableDesc.getDeserializerClass();
    String[] schemaProps = AnnotationUtils.getAnnotation(tableSerDe, SerDeSpec.class).schemaProps();
    Properties tableProps = tableDesc.getProperties();
    for (PartitionDesc partitionDesc : partDescs) {
      if (!tableSerDe.getName().equals(partitionDesc.getDeserializerClassName())) {
        return true;
      }
      Properties partProps = partitionDesc.getProperties();
      for (String schemaProp : schemaProps) {
        if (!org.apache.commons.lang3.StringUtils.equals(
            tableProps.getProperty(schemaProp), partProps.getProperty(schemaProp))) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Lists status for all files under a given path. Whether or not this is recursive depends on the
   * setting of job configuration parameter mapred.input.dir.recursive.
   *
   * @param fs
   *          file system
   *
   * @param p
   *          path in file system
   *
   * @return list of file status entries
   */
  private FileStatus[] listStatusUnderPath(FileSystem fs, Path p) throws IOException {
    boolean recursive = HiveConf.getBoolVar(job, HiveConf.ConfVars.HADOOPMAPREDINPUTDIRRECURSIVE);
    // If this is in acid format always read it recursively regardless of what the jobconf says.
    if (!recursive && !AcidUtils.isAcid(p, job)) {
      return fs.listStatus(p, FileUtils.HIDDEN_FILES_PATH_FILTER);
    }
    List<FileStatus> results = new ArrayList<FileStatus>();
    for (FileStatus stat : fs.listStatus(p, FileUtils.HIDDEN_FILES_PATH_FILTER)) {
      FileUtils.listStatusRecursively(fs, stat, results);
    }
    return results.toArray(new FileStatus[results.size()]);
  }

  // for split sampling. shrinkedLength is checked against IOContext.getCurrentBlockStart,
  // which is from RecordReader.getPos(). So some inputformats which does not support getPos()
  // like HiveHBaseTableInputFormat cannot be used with this (todo)
  private static class FetchInputFormatSplit extends HiveInputFormat.HiveInputSplit {

    // shrinked size for this split. counter part of this in normal mode is
    // InputSplitShim.shrinkedLength.
    // what's different is that this is evaluated by unit of row using RecordReader.getPos()
    // and that is evaluated by unit of split using InputSplit.getLength().
    private long shrinkedLength = -1;
    private InputFormat inputFormat;

    public FetchInputFormatSplit(InputSplit split, InputFormat inputFormat) {
      super(split, inputFormat.getClass().getName());
      this.inputFormat = inputFormat;
    }

    public RecordReader<WritableComparable, Writable> getRecordReader(JobConf job) throws IOException {
      return inputFormat.getRecordReader(getInputSplit(), job, Reporter.NULL);
    }
  }
}
