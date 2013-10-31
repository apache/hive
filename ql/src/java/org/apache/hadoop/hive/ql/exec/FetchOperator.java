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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
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
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.DelegatedObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * FetchTask implementation.
 **/
public class FetchOperator implements Serializable {

  static Log LOG = LogFactory.getLog(FetchOperator.class.getName());
  static LogHelper console = new LogHelper(LOG);

  private boolean isNativeTable;
  private FetchWork work;
  protected Operator<?> operator;    // operator tree for processing row further (option)
  private int splitNum;
  private PartitionDesc currPart;
  private TableDesc currTbl;
  private boolean tblDataDone;

  private boolean hasVC;
  private boolean isPartitioned;
  private StructObjectInspector vcsOI;
  private List<VirtualColumn> vcCols;
  private ExecMapperContext context;

  private transient RecordReader<WritableComparable, Writable> currRecReader;
  private transient FetchInputFormatSplit[] inputSplits;
  private transient InputFormat inputFormat;
  private transient JobConf job;
  private transient WritableComparable key;
  private transient Writable value;
  private transient Writable[] vcValues;
  private transient Deserializer serde;
  private transient Deserializer tblSerde;
  private transient Converter partTblObjectInspectorConverter;

  private transient Iterator<Path> iterPath;
  private transient Iterator<PartitionDesc> iterPartDesc;
  private transient Path currPath;
  private transient StructObjectInspector objectInspector;
  private transient StructObjectInspector rowObjectInspector;
  private transient ObjectInspector partitionedTableOI;
  private transient Object[] row;

  public FetchOperator() {
  }

  public FetchOperator(FetchWork work, JobConf job) {
    this.job = job;
    this.work = work;
    initialize();
  }

  public FetchOperator(FetchWork work, JobConf job, Operator<?> operator,
      List<VirtualColumn> vcCols) {
    this.job = job;
    this.work = work;
    this.operator = operator;
    this.vcCols = vcCols;
    initialize();
  }

  private void initialize() {
    if (hasVC = vcCols != null && !vcCols.isEmpty()) {
      List<String> names = new ArrayList<String>(vcCols.size());
      List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(vcCols.size());
      for (VirtualColumn vc : vcCols) {
        inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                vc.getTypeInfo()));
        names.add(vc.getName());
      }
      vcsOI = ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
      vcValues = new Writable[vcCols.size()];
    }
    isPartitioned = work.isPartitioned();
    tblDataDone = false;
    if (hasVC && isPartitioned) {
      row = new Object[3];
    } else if (hasVC || isPartitioned) {
      row = new Object[2];
    } else {
      row = new Object[1];
    }
    if (work.getTblDesc() != null) {
      isNativeTable = !work.getTblDesc().isNonNative();
    } else {
      isNativeTable = true;
    }
    setupExecContext();
  }

  private void setupExecContext() {
    if (hasVC || work.getSplitSample() != null) {
      context = new ExecMapperContext();
      if (operator != null) {
        operator.setExecContext(context);
      }
    }
  }

  public FetchWork getWork() {
    return work;
  }

  public void setWork(FetchWork work) {
    this.work = work;
  }

  public int getSplitNum() {
    return splitNum;
  }

  public void setSplitNum(int splitNum) {
    this.splitNum = splitNum;
  }

  public PartitionDesc getCurrPart() {
    return currPart;
  }

  public void setCurrPart(PartitionDesc currPart) {
    this.currPart = currPart;
  }

  public TableDesc getCurrTbl() {
    return currTbl;
  }

  public void setCurrTbl(TableDesc currTbl) {
    this.currTbl = currTbl;
  }

  public boolean isTblDataDone() {
    return tblDataDone;
  }

  public void setTblDataDone(boolean tblDataDone) {
    this.tblDataDone = tblDataDone;
  }

  public boolean isEmptyTable() {
    return work.getTblDir() == null && (work.getPartDir() == null || work.getPartDir().isEmpty());
  }

  /**
   * A cache of InputFormat instances.
   */
  private static Map<Class, InputFormat<WritableComparable, Writable>> inputFormats = new HashMap<Class, InputFormat<WritableComparable, Writable>>();

  @SuppressWarnings("unchecked")
  static InputFormat<WritableComparable, Writable> getInputFormatFromCache(Class inputFormatClass,
      Configuration conf) throws IOException {
    if (!inputFormats.containsKey(inputFormatClass)) {
      try {
        InputFormat<WritableComparable, Writable> newInstance = (InputFormat<WritableComparable, Writable>) ReflectionUtils
            .newInstance(inputFormatClass, conf);
        inputFormats.put(inputFormatClass, newInstance);
      } catch (Exception e) {
        throw new IOException("Cannot create an instance of InputFormat class "
            + inputFormatClass.getName() + " as specified in mapredWork!", e);
      }
    }
    return inputFormats.get(inputFormatClass);
  }

  private StructObjectInspector getRowInspectorFromTable(TableDesc table) throws Exception {
    Deserializer serde = table.getDeserializerClass().newInstance();
    serde.initialize(job, table.getProperties());
    return createRowInspector(getStructOIFrom(serde.getObjectInspector()));
  }

  private StructObjectInspector getRowInspectorFromPartition(PartitionDesc partition,
      ObjectInspector partitionOI) throws Exception {

    String pcols = partition.getTableDesc().getProperties().getProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    String[] partKeys = pcols.trim().split("/");
    row[1] = createPartValue(partKeys, partition.getPartSpec());

    return createRowInspector(getStructOIFrom(partitionOI), partKeys);
  }

  private StructObjectInspector getRowInspectorFromPartitionedTable(TableDesc table)
      throws Exception {
    Deserializer serde = table.getDeserializerClass().newInstance();
    serde.initialize(job, table.getProperties());
    String pcols = table.getProperties().getProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    String[] partKeys = pcols.trim().split("/");
    row[1] = null;
    return createRowInspector(getStructOIFrom(serde.getObjectInspector()), partKeys);
  }

  private StructObjectInspector getStructOIFrom(ObjectInspector current) throws SerDeException {
    if (objectInspector != null) {
      current = DelegatedObjectInspectorFactory.reset(objectInspector, current);
    } else {
      current = DelegatedObjectInspectorFactory.wrap(current);
    }
    return objectInspector = (StructObjectInspector) current;
  }

  private StructObjectInspector createRowInspector(StructObjectInspector current)
      throws SerDeException {
    return hasVC ? ObjectInspectorFactory.getUnionStructObjectInspector(
        Arrays.asList(current, vcsOI)) : current;
  }

  private StructObjectInspector createRowInspector(StructObjectInspector current, String[] partKeys)
      throws SerDeException {
    List<String> partNames = new ArrayList<String>();
    List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>();
    for (String key : partKeys) {
      partNames.add(key);
      partObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
    StructObjectInspector partObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(partNames, partObjectInspectors);

    return ObjectInspectorFactory.getUnionStructObjectInspector(
        hasVC ? Arrays.asList(current, partObjectInspector, vcsOI) :
            Arrays.asList(current, partObjectInspector));
  }

  private List<String> createPartValue(String[] partKeys, Map<String, String> partSpec) {
    List<String> partValues = new ArrayList<String>();
    for (String key : partKeys) {
      partValues.add(partSpec.get(key));
    }
    return partValues;
  }

  private void getNextPath() throws Exception {
    // first time
    if (iterPath == null) {
      if (work.isNotPartitioned()) {
        if (!tblDataDone) {
          currPath = work.getTblDirPath();
          currTbl = work.getTblDesc();
          if (isNativeTable) {
            FileSystem fs = currPath.getFileSystem(job);
            if (fs.exists(currPath)) {
              FileStatus[] fStats = listStatusUnderPath(fs, currPath);
              for (FileStatus fStat : fStats) {
                if (fStat.getLen() > 0) {
                  tblDataDone = true;
                  break;
                }
              }
            }
          } else {
            tblDataDone = true;
          }

          if (!tblDataDone) {
            currPath = null;
          }
          return;
        } else {
          currTbl = null;
          currPath = null;
        }
        return;
      } else {
        iterPath = FetchWork.convertStringToPathArray(work.getPartDir()).iterator();
        iterPartDesc = work.getPartDesc().iterator();
      }
    }

    while (iterPath.hasNext()) {
      Path nxt = iterPath.next();
      PartitionDesc prt = null;
      if (iterPartDesc != null) {
        prt = iterPartDesc.next();
      }
      FileSystem fs = nxt.getFileSystem(job);
      if (fs.exists(nxt)) {
        FileStatus[] fStats = listStatusUnderPath(fs, nxt);
        for (FileStatus fStat : fStats) {
          if (fStat.getLen() > 0) {
            currPath = nxt;
            if (iterPartDesc != null) {
              currPart = prt;
            }
            return;
          }
        }
      }
    }
  }

  /**
   * A cache of Object Inspector Settable Properties.
   */
  private static Map<ObjectInspector, Boolean> oiSettableProperties = new HashMap<ObjectInspector, Boolean>();

  private RecordReader<WritableComparable, Writable> getRecordReader() throws Exception {
    if (currPath == null) {
      getNextPath();
      if (currPath == null) {
        return null;
      }

      // not using FileInputFormat.setInputPaths() here because it forces a
      // connection
      // to the default file system - which may or may not be online during pure
      // metadata
      // operations
      job.set("mapred.input.dir", org.apache.hadoop.util.StringUtils.escapeString(currPath
          .toString()));

      // Fetch operator is not vectorized and as such turn vectorization flag off so that
      // non-vectorized record reader is created below.
      if (HiveConf.getBoolVar(job, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED)) {
        HiveConf.setBoolVar(job, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
      }

      PartitionDesc partDesc;
      if (currTbl == null) {
        partDesc = currPart;
      } else {
        partDesc = new PartitionDesc(currTbl, null);
      }

      Class<? extends InputFormat> formatter = partDesc.getInputFileFormatClass();
      inputFormat = getInputFormatFromCache(formatter, job);
      Utilities.copyTableJobPropertiesToConf(partDesc.getTableDesc(), job);
      InputSplit[] splits = inputFormat.getSplits(job, 1);
      FetchInputFormatSplit[] inputSplits = new FetchInputFormatSplit[splits.length];
      for (int i = 0; i < splits.length; i++) {
        inputSplits[i] = new FetchInputFormatSplit(splits[i], formatter.getName());
      }
      if (work.getSplitSample() != null) {
        inputSplits = splitSampling(work.getSplitSample(), inputSplits);
      }
      this.inputSplits = inputSplits;

      splitNum = 0;
      serde = partDesc.getDeserializer();
      serde.initialize(job, partDesc.getOverlayedProperties());

      if (currTbl != null) {
        tblSerde = serde;
      }
      else {
        tblSerde = currPart.getTableDesc().getDeserializerClass().newInstance();
        tblSerde.initialize(job, currPart.getTableDesc().getProperties());
      }

      ObjectInspector outputOI = ObjectInspectorConverters.getConvertedOI(
          serde.getObjectInspector(),
          partitionedTableOI == null ? tblSerde.getObjectInspector() : partitionedTableOI,
          oiSettableProperties);

      partTblObjectInspectorConverter = ObjectInspectorConverters.getConverter(
          serde.getObjectInspector(), outputOI);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating fetchTask with deserializer typeinfo: "
            + serde.getObjectInspector().getTypeName());
        LOG.debug("deserializer properties: " + partDesc.getOverlayedProperties());
      }

      if (currPart != null) {
        getRowInspectorFromPartition(currPart, outputOI);
      }
    }

    if (splitNum >= inputSplits.length) {
      if (currRecReader != null) {
        currRecReader.close();
        currRecReader = null;
      }
      currPath = null;
      return getRecordReader();
    }

    final FetchInputFormatSplit target = inputSplits[splitNum];

    @SuppressWarnings("unchecked")
    final RecordReader<WritableComparable, Writable> reader =
        inputFormat.getRecordReader(target.getInputSplit(), job, Reporter.NULL);
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
          initIOContext(target, job, inputFormat.getClass(), reader);
    } else {
      currRecReader = reader;
    }
    splitNum++;
    key = currRecReader.createKey();
    value = currRecReader.createValue();
    return currRecReader;
  }

  private FetchInputFormatSplit[] splitSampling(SplitSample splitSample,
      FetchInputFormatSplit[] splits) {
    long totalSize = 0;
    for (FetchInputFormatSplit split: splits) {
        totalSize += split.getLength();
    }
    List<FetchInputFormatSplit> result = new ArrayList<FetchInputFormatSplit>();
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
    if(work.getRowsComputedUsingStats() != null) {
      for (List<Object> row : work.getRowsComputedUsingStats()) {
        operator.process(row, 0);
      }
      operator.flush();
      return true;
    }
    InspectableObject row = getNextRow();
    if (row != null) {
      pushRow(row);
    } else {
      operator.flush();
    }
    return row != null;
  }

  protected void pushRow(InspectableObject row) throws HiveException {
    operator.process(row.o, 0);
  }

  private transient final InspectableObject inspectable = new InspectableObject();

  /**
   * Get the next row. The fetch context is modified appropriately.
   *
   **/
  public InspectableObject getNextRow() throws IOException {
    try {
      while (true) {
        if (context != null) {
          context.resetRow();
        }
        if (currRecReader == null) {
          currRecReader = getRecordReader();
          if (currRecReader == null) {
            return null;
          }
        }

        boolean ret = currRecReader.next(key, value);
        if (ret) {
          if (operator != null && context != null && context.inputFileChanged()) {
            // The child operators cleanup if input file has changed
            try {
              operator.cleanUpInputFileChanged();
            } catch (HiveException e) {
              throw new IOException(e);
            }
          }
          if (hasVC) {
            vcValues = MapOperator.populateVirtualColumnValues(context, vcCols, vcValues, serde);
            row[isPartitioned ? 2 : 1] = vcValues;
          }
          row[0] = partTblObjectInspectorConverter.convert(serde.deserialize(value));

          if (hasVC || isPartitioned) {
            inspectable.o = row;
            inspectable.oi = rowObjectInspector;
            return inspectable;
          }
          inspectable.o = row[0];
          inspectable.oi = tblSerde.getObjectInspector();
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
      this.currTbl = null;
      this.currPath = null;
      this.iterPath = null;
      this.iterPartDesc = null;
    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  /**
   * used for bucket map join
   */
  public void setupContext(List<Path> paths) {
    this.iterPath = paths.iterator();
    if (work.isNotPartitioned()) {
      this.currTbl = work.getTblDesc();
    } else {
      this.iterPartDesc = work.getPartDescs(paths).iterator();
    }
    setupExecContext();
  }

  /**
   * returns output ObjectInspector, never null
   */
  public ObjectInspector getOutputObjectInspector() throws HiveException {
    if(null != work.getStatRowOI()) {
      return work.getStatRowOI();
    }
    try {
      if (work.isNotPartitioned()) {
        return getRowInspectorFromTable(work.getTblDesc());
      }
      List<PartitionDesc> listParts = work.getPartDesc();
      // Chose the table descriptor if none of the partitions is present.
      // For eg: consider the query:
      // select /*+mapjoin(T1)*/ count(*) from T1 join T2 on T1.key=T2.key
      // Both T1 and T2 and partitioned tables, but T1 does not have any partitions
      // FetchOperator is invoked for T1, and listParts is empty. In that case,
      // use T1's schema to get the ObjectInspector.
      if (listParts == null || listParts.isEmpty()) {
        return getRowInspectorFromPartitionedTable(work.getTblDesc());
      }

      // Choose any partition. It's OI needs to be converted to the table OI
      // Whenever a new partition is being read, a new converter is being created
      PartitionDesc partition = listParts.get(0);
      Deserializer tblSerde = partition.getTableDesc().getDeserializerClass().newInstance();
      tblSerde.initialize(job, partition.getTableDesc().getProperties());

      partitionedTableOI = null;
      ObjectInspector tableOI = tblSerde.getObjectInspector();

      // Get the OI corresponding to all the partitions
      for (PartitionDesc listPart : listParts) {
        partition = listPart;
        Deserializer partSerde = listPart.getDeserializer();
        partSerde.initialize(job, listPart.getOverlayedProperties());

        partitionedTableOI = ObjectInspectorConverters.getConvertedOI(
            partSerde.getObjectInspector(), tableOI, oiSettableProperties);
        if (!partitionedTableOI.equals(tableOI)) {
          break;
        }
      }
      return getRowInspectorFromPartition(partition, partitionedTableOI);
    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    } finally {
      currPart = null;
    }
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
    if (!recursive) {
      return fs.listStatus(p);
    }
    List<FileStatus> results = new ArrayList<FileStatus>();
    for (FileStatus stat : fs.listStatus(p)) {
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
    // and that is evaluated by unit of split using InputSplt.getLength().
    private long shrinkedLength = -1;

    public FetchInputFormatSplit(InputSplit split, String name) {
      super(split, name);
    }
  }
}
