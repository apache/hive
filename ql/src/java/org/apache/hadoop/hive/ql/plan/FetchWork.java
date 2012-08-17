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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.parse.SplitSample;

/**
 * FetchWork.
 *
 */
@Explain(displayName = "Fetch Operator")
public class FetchWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private String tblDir;
  private TableDesc tblDesc;

  private ArrayList<String> partDir;
  private ArrayList<PartitionDesc> partDesc;

  private Operator<?> source;
  private ListSinkOperator sink;

  private int limit;
  private int leastNumRows;

  private SplitSample splitSample;

  /**
   * Serialization Null Format for the serde used to fetch data.
   */
  private String serializationNullFormat = "NULL";

  public FetchWork() {
  }

  public FetchWork(String tblDir, TableDesc tblDesc) {
    this(tblDir, tblDesc, -1);
  }

  public FetchWork(String tblDir, TableDesc tblDesc, int limit) {
    this.tblDir = tblDir;
    this.tblDesc = tblDesc;
    this.limit = limit;
  }

  public FetchWork(List<String> partDir, List<PartitionDesc> partDesc, TableDesc tblDesc) {
    this(partDir, partDesc, tblDesc, -1);
  }

  public FetchWork(List<String> partDir, List<PartitionDesc> partDesc,
      TableDesc tblDesc, int limit) {
    this.tblDesc = tblDesc;
    this.partDir = new ArrayList<String>(partDir);
    this.partDesc = new ArrayList<PartitionDesc>(partDesc);
    this.limit = limit;
  }

  public void initializeForFetch() {
    if (source == null) {
      sink = new ListSinkOperator();
      sink.setConf(new ListSinkDesc(serializationNullFormat));
      source = sink;
    }
  }

  public String getSerializationNullFormat() {
    return serializationNullFormat;
  }

  public void setSerializationNullFormat(String format) {
    serializationNullFormat = format;
  }

  public boolean isNotPartitioned() {
    return tblDir != null;
  }

  public boolean isPartitioned() {
    return tblDir == null;
  }

  /**
   * @return the tblDir
   */
  public String getTblDir() {
    return tblDir;
  }

  /**
   * @return the tblDir
   */
  public Path getTblDirPath() {
    return new Path(tblDir);
  }

  /**
   * @param tblDir
   *          the tblDir to set
   */
  public void setTblDir(String tblDir) {
    this.tblDir = tblDir;
  }

  /**
   * @return the tblDesc
   */
  public TableDesc getTblDesc() {
    return tblDesc;
  }

  /**
   * @param tblDesc
   *          the tblDesc to set
   */
  public void setTblDesc(TableDesc tblDesc) {
    this.tblDesc = tblDesc;
  }

  /**
   * @return the partDir
   */
  public ArrayList<String> getPartDir() {
    return partDir;
  }

  public List<Path> getPartDirPath() {
    return FetchWork.convertStringToPathArray(partDir);
  }

  public static List<String> convertPathToStringArray(List<Path> paths) {
    if (paths == null) {
      return null;
    }

    List<String> pathsStr = new ArrayList<String>();
    for (Path path : paths) {
      pathsStr.add(path.toString());
    }

    return pathsStr;
  }

  public static List<Path> convertStringToPathArray(List<String> paths) {
    if (paths == null) {
      return null;
    }

    List<Path> pathsStr = new ArrayList<Path>();
    for (String path : paths) {
      pathsStr.add(new Path(path));
    }

    return pathsStr;
  }

  /**
   * @param partDir
   *          the partDir to set
   */
  public void setPartDir(ArrayList<String> partDir) {
    this.partDir = partDir;
  }

  /**
   * @return the partDesc
   */
  public ArrayList<PartitionDesc> getPartDesc() {
    return partDesc;
  }

  /**
   * @param partDesc
   *          the partDesc to set
   */
  public void setPartDesc(ArrayList<PartitionDesc> partDesc) {
    this.partDesc = partDesc;
  }

  /**
   * @return the limit
   */
  @Explain(displayName = "limit")
  public int getLimit() {
    return limit;
  }

  /**
   * @param limit
   *          the limit to set
   */
  public void setLimit(int limit) {
    this.limit = limit;
  }

  public int getLeastNumRows() {
    return leastNumRows;
  }

  public void setLeastNumRows(int leastNumRows) {
    this.leastNumRows = leastNumRows;
  }

  @Explain(displayName = "Processor Tree")
  public Operator<?> getSource() {
    return source;
  }

  public void setSource(Operator<?> source) {
    this.source = source;
  }

  public ListSinkOperator getSink() {
    return sink;
  }

  public void setSink(ListSinkOperator sink) {
    this.sink = sink;
  }

  public void setSplitSample(SplitSample splitSample) {
    this.splitSample = splitSample;
  }

  public SplitSample getSplitSample() {
    return splitSample;
  }

  @Override
  public String toString() {
    if (tblDir != null) {
      return new String("table = " + tblDir);
    }

    if (partDir == null) {
      return "null fetchwork";
    }

    String ret = "partition = ";
    for (String part : partDir) {
      ret = ret.concat(part);
    }

    return ret;
  }
}
