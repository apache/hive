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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
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

  public FetchOperator() {
  }

  public FetchOperator(FetchWork work, JobConf job) {
    this.work = work;
    initialize(job);
  }

  public void initialize(JobConf job) {
    this.job = job;
    tblDataDone = false;
    rowWithPart = new Object[2];
    if (work.getTblDesc() != null) {
      isNativeTable = !work.getTblDesc().isNonNative();
    } else {
      isNativeTable = true;
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

  private boolean isNativeTable;
  private FetchWork work;
  private int splitNum;
  private PartitionDesc currPart;
  private TableDesc currTbl;
  private boolean tblDataDone;

  private transient RecordReader<WritableComparable, Writable> currRecReader;
  private transient InputSplit[] inputSplits;
  private transient InputFormat inputFormat;
  private transient JobConf job;
  private transient WritableComparable key;
  private transient Writable value;
  private transient Deserializer serde;
  private transient Iterator<Path> iterPath;
  private transient Iterator<PartitionDesc> iterPartDesc;
  private transient Path currPath;
  private transient StructObjectInspector rowObjectInspector;
  private transient Object[] rowWithPart;

  /**
   * A cache of InputFormat instances.
   */
  private static Map<Class, InputFormat<WritableComparable, Writable>> inputFormats =
      new HashMap<Class, InputFormat<WritableComparable, Writable>>();

  static InputFormat<WritableComparable, Writable> getInputFormatFromCache(
      Class inputFormatClass, Configuration conf) throws IOException {
    if (!inputFormats.containsKey(inputFormatClass)) {
      try {
        InputFormat<WritableComparable, Writable> newInstance =
          (InputFormat<WritableComparable, Writable>) ReflectionUtils
            .newInstance(inputFormatClass, conf);
        inputFormats.put(inputFormatClass, newInstance);
      } catch (Exception e) {
        throw new IOException("Cannot create an instance of InputFormat class "
            + inputFormatClass.getName() + " as specified in mapredWork!");
      }
    }
    return inputFormats.get(inputFormatClass);
  }

  private void setPrtnDesc() throws Exception {
    List<String> partNames = new ArrayList<String>();
    List<String> partValues = new ArrayList<String>();

    String pcols = currPart
        .getTableDesc()
        .getProperties()
        .getProperty(
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS);
    LinkedHashMap<String, String> partSpec = currPart.getPartSpec();

    List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>();
    String[] partKeys = pcols.trim().split("/");
    for (String key : partKeys) {
      partNames.add(key);
      partValues.add(partSpec.get(key));
      partObjectInspectors
          .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
    StructObjectInspector partObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(partNames, partObjectInspectors);
    rowObjectInspector = (StructObjectInspector) serde.getObjectInspector();

    rowWithPart[1] = partValues;
    rowObjectInspector = ObjectInspectorFactory
        .getUnionStructObjectInspector(Arrays
        .asList(new StructObjectInspector[] {rowObjectInspector, partObjectInspector}));
  }

  private void getNextPath() throws Exception {
    // first time
    if (iterPath == null) {
      if (work.getTblDir() != null) {
        if (!tblDataDone) {
          currPath = work.getTblDirPath();
          currTbl = work.getTblDesc();
          if (isNativeTable) {
            FileSystem fs = currPath.getFileSystem(job);
            if (fs.exists(currPath)) {
              FileStatus[] fStats = fs.listStatus(currPath);
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
        iterPath = FetchWork.convertStringToPathArray(work.getPartDir())
            .iterator();
        iterPartDesc = work.getPartDesc().iterator();
      }
    }

    while (iterPath.hasNext()) {
      Path nxt = iterPath.next();
      PartitionDesc prt = null;
      if(iterPartDesc != null)
        prt = iterPartDesc.next();
      FileSystem fs = nxt.getFileSystem(job);
      if (fs.exists(nxt)) {
        FileStatus[] fStats = fs.listStatus(nxt);
        for (FileStatus fStat : fStats) {
          if (fStat.getLen() > 0) {
            currPath = nxt;
            if(iterPartDesc != null) {
              currPart = prt;
            }
            return;
          }
        }
      }
    }
  }

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
      job.set("mapred.input.dir", org.apache.hadoop.util.StringUtils
          .escapeString(currPath.toString()));

      TableDesc tmp = currTbl;
      if (tmp == null) {
        tmp = currPart.getTableDesc();
      }
      inputFormat = getInputFormatFromCache(tmp.getInputFileFormatClass(), job);
      Utilities.copyTableJobPropertiesToConf(tmp, job);
      inputSplits = inputFormat.getSplits(job, 1);
      splitNum = 0;
      serde = tmp.getDeserializerClass().newInstance();
      serde.initialize(job, tmp.getProperties());
      LOG.debug("Creating fetchTask with deserializer typeinfo: "
          + serde.getObjectInspector().getTypeName());
      LOG.debug("deserializer properties: " + tmp.getProperties());
      if (currPart != null) {
        setPrtnDesc();
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

    currRecReader = inputFormat.getRecordReader(inputSplits[splitNum++], job,
        Reporter.NULL);
    key = currRecReader.createKey();
    value = currRecReader.createValue();
    return currRecReader;
  }

  /**
   * Get the next row. The fetch context is modified appropriately.
   *
   **/
  public InspectableObject getNextRow() throws IOException {
    try {
      while (true) {
        if (currRecReader == null) {
          currRecReader = getRecordReader();
          if (currRecReader == null) {
            return null;
          }
        }

        boolean ret = currRecReader.next(key, value);
        if (ret) {
          if (this.currPart == null) {
            Object obj = serde.deserialize(value);
            return new InspectableObject(obj, serde.getObjectInspector());
          } else {
            rowWithPart[0] = serde.deserialize(value);
            return new InspectableObject(rowWithPart, rowObjectInspector);
          }
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
      this.currPath = null;
      this.iterPath = null;
      this.iterPartDesc = null;
    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  /**
   * used for bucket map join. there is a hack for getting partitionDesc.
   * bucket map join right now only allow one partition present in bucket map join.
   */
  public void setupContext (Iterator<Path> iterPath, Iterator<PartitionDesc> iterPartDesc) {
    this.iterPath = iterPath;
    this.iterPartDesc = iterPartDesc;
    if(iterPartDesc == null) {
      if (work.getTblDir() != null) {
        this.currTbl = work.getTblDesc();
      } else {
        //hack, get the first.
        List<PartitionDesc> listParts = work.getPartDesc();
        currPart = listParts.get(0);
      }
    }
  }

  public ObjectInspector getOutputObjectInspector() throws HiveException {
    try {
      if (work.getTblDir() != null) {
        TableDesc tbl = work.getTblDesc();
        Deserializer serde = tbl.getDeserializerClass().newInstance();
        serde.initialize(job, tbl.getProperties());
        return serde.getObjectInspector();
      } else {
        List<PartitionDesc> listParts = work.getPartDesc();
        currPart = listParts.get(0);
        serde = currPart.getTableDesc().getDeserializerClass().newInstance();
        serde.initialize(job, currPart.getTableDesc().getProperties());
        setPrtnDesc();
        currPart = null;
        return rowObjectInspector;
      }
    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }
}
