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

@explain(displayName = "Fetch Operator")
public class fetchWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private String tblDir;
  private tableDesc tblDesc;

  private List<String> partDir;
  private List<partitionDesc> partDesc;

  private int limit;

  /**
   * Serialization Null Format for the serde used to fetch data
   */
  private String serializationNullFormat = "NULL";

  public fetchWork() {
  }

  public fetchWork(String tblDir, tableDesc tblDesc) {
    this(tblDir, tblDesc, -1);
  }

  public fetchWork(String tblDir, tableDesc tblDesc, int limit) {
    this.tblDir = tblDir;
    this.tblDesc = tblDesc;
    this.limit = limit;
  }

  public fetchWork(List<String> partDir, List<partitionDesc> partDesc) {
    this(partDir, partDesc, -1);
  }

  public fetchWork(List<String> partDir, List<partitionDesc> partDesc, int limit) {
    this.partDir = partDir;
    this.partDesc = partDesc;
    this.limit = limit;
  }

  public String getSerializationNullFormat() {
    return serializationNullFormat;
  }

  public void setSerializationNullFormat(String format) {
    serializationNullFormat = format;
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
  public tableDesc getTblDesc() {
    return tblDesc;
  }

  /**
   * @param tblDesc
   *          the tblDesc to set
   */
  public void setTblDesc(tableDesc tblDesc) {
    this.tblDesc = tblDesc;
  }

  /**
   * @return the partDir
   */
  public List<String> getPartDir() {
    return partDir;
  }

  public List<Path> getPartDirPath() {
    return fetchWork.convertStringToPathArray(partDir);
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
  public void setPartDir(List<String> partDir) {
    this.partDir = partDir;
  }

  /**
   * @return the partDesc
   */
  public List<partitionDesc> getPartDesc() {
    return partDesc;
  }

  /**
   * @param partDesc
   *          the partDesc to set
   */
  public void setPartDesc(List<partitionDesc> partDesc) {
    this.partDesc = partDesc;
  }

  /**
   * @return the limit
   */
  @explain(displayName = "limit")
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

  @Override
  public String toString() {
    if (tblDir != null) {
      return new String("table = " + tblDir);
    }

    if (partDir == null) {
      return "null fetchwork";
    }

    String ret = new String("partition = ");
    for (String part : partDir) {
      ret = ret.concat(part);
    }

    return ret;
  }
}
