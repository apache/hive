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
 * FetchTask implementation
 **/
public class FetchOperator {

  transient protected Log LOG;
  transient protected LogHelper console;

  public FetchOperator(FetchWork work, JobConf job) {
    LOG = LogFactory.getLog(this.getClass().getName());
    console = new LogHelper(LOG);

    this.work = work;
    this.job = job;

    currRecReader = null;
    currPath = null;
    currTbl = null;
    currPart = null;
    iterPath = null;
    iterPartDesc = null;
    tblDataDone = false;
    rowWithPart = new Object[2];
  }

  private final FetchWork work;
  private int splitNum;
  private RecordReader<WritableComparable, Writable> currRecReader;
  private InputSplit[] inputSplits;
  private InputFormat inputFormat;
  private final JobConf job;
  private WritableComparable key;
  private Writable value;
  private Deserializer serde;
  private Iterator<Path> iterPath;
  private Iterator<PartitionDesc> iterPartDesc;
  private Path currPath;
  private PartitionDesc currPart;
  private TableDesc currTbl;
  private boolean tblDataDone;
  private StructObjectInspector rowObjectInspector;
  private final Object[] rowWithPart;

  /**
   * A cache of InputFormat instances.
   */
  private static Map<Class, InputFormat<WritableComparable, Writable>> inputFormats = new HashMap<Class, InputFormat<WritableComparable, Writable>>();

  static InputFormat<WritableComparable, Writable> getInputFormatFromCache(
      Class inputFormatClass, Configuration conf) throws IOException {
    if (!inputFormats.containsKey(inputFormatClass)) {
      try {
        InputFormat<WritableComparable, Writable> newInstance = (InputFormat<WritableComparable, Writable>) ReflectionUtils
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
            .asList(new StructObjectInspector[] { rowObjectInspector,
                partObjectInspector }));
  }

  private void getNextPath() throws Exception {
    // first time
    if (iterPath == null) {
      if (work.getTblDir() != null) {
        if (!tblDataDone) {
          currPath = work.getTblDirPath();
          currTbl = work.getTblDesc();
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
      PartitionDesc prt = iterPartDesc.next();
      FileSystem fs = nxt.getFileSystem(job);
      if (fs.exists(nxt)) {
        FileStatus[] fStats = fs.listStatus(nxt);
        for (FileStatus fStat : fStats) {
          if (fStat.getLen() > 0) {
            currPath = nxt;
            currPart = prt;
            return;
          }
        }
      }
    }
  }

  private RecordReader<WritableComparable, Writable> getRecordReader()
      throws Exception {
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
      inputSplits = inputFormat.getSplits(job, 1);
      splitNum = 0;
      serde = tmp.getDeserializerClass().newInstance();
      serde.initialize(job, tmp.getProperties());
      LOG.debug("Creating fetchTask with deserializer typeinfo: "
          + serde.getObjectInspector().getTypeName());
      LOG.debug("deserializer properties: " + tmp.getProperties());
      if (!tblDataDone) {
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
      if (currRecReader == null) {
        currRecReader = getRecordReader();
        if (currRecReader == null) {
          return null;
        }
      }

      boolean ret = currRecReader.next(key, value);
      if (ret) {
        if (tblDataDone) {
          Object obj = serde.deserialize(value);
          return new InspectableObject(obj, serde.getObjectInspector());
        } else {
          rowWithPart[0] = serde.deserialize(value);
          return new InspectableObject(rowWithPart, rowObjectInspector);
        }
      } else {
        currRecReader.close();
        currRecReader = null;
        currRecReader = getRecordReader();
        if (currRecReader == null) {
          return null;
        } else {
          return getNextRow();
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
    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
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
