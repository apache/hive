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

package org.apache.hadoop.hive.ql.io.merge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcFileStripeMergeInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileBlockMergeInputFormat;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.mapred.InputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Explain(displayName = "Merge File Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class MergeFileWork extends MapWork {

  private static final Logger LOG = LoggerFactory.getLogger(MergeFileWork.class);
  private Path outputDir;
  private boolean hasDynamicPartitions;
  private boolean isListBucketingAlterTableConcatenate;
  private ListBucketingCtx listBucketingCtx;

  // source table input format
  private String srcTblInputFormat;

  // internal input format used by CombineHiveInputFormat
  private Class<? extends InputFormat> internalInputFormat;

  public MergeFileWork(List<Path> inputPaths, Path outputDir,
      String srcTblInputFormat, TableDesc tbl) {
    this(inputPaths, outputDir, false, srcTblInputFormat, tbl);
  }

  public MergeFileWork(List<Path> inputPaths, Path outputDir,
      boolean hasDynamicPartitions,
      String srcTblInputFormat, TableDesc tbl) {
    this.inputPaths = inputPaths;
    this.outputDir = outputDir;
    this.hasDynamicPartitions = hasDynamicPartitions;
    this.srcTblInputFormat = srcTblInputFormat;
    PartitionDesc partDesc = new PartitionDesc();
    if (srcTblInputFormat.equals(OrcInputFormat.class.getName())) {
      this.internalInputFormat = OrcFileStripeMergeInputFormat.class;
    } else if (srcTblInputFormat.equals(RCFileInputFormat.class.getName())) {
      this.internalInputFormat = RCFileBlockMergeInputFormat.class;
    }
    partDesc.setInputFileFormatClass(internalInputFormat);
    partDesc.setTableDesc(tbl);
    for (Path path : this.inputPaths) {
      this.addPathToPartitionInfo(path, partDesc);
    }
    this.isListBucketingAlterTableConcatenate = false;
  }

  public Path getOutputDir() {
    return outputDir;
  }

  public void setOutputDir(Path outputDir) {
    this.outputDir = outputDir;
  }

  @Override
  public Long getMinSplitSize() {
    return null;
  }

  @Override
  public String getInputformat() {
    return getInputformatClass().getName();
  }

  public Class<? extends InputFormat> getInputformatClass() {
    return CombineHiveInputFormat.class;
  }

  @Override
  public boolean isGatheringStats() {
    return false;
  }

  public boolean hasDynamicPartitions() {
    return this.hasDynamicPartitions;
  }

  public void setHasDynamicPartitions(boolean hasDynamicPartitions) {
    this.hasDynamicPartitions = hasDynamicPartitions;
  }

  @Override
  public void resolveDynamicPartitionStoredAsSubDirsMerge(HiveConf conf,
      Path path,
      TableDesc tblDesc,
      List<String> aliases,
      PartitionDesc partDesc) {
    super.resolveDynamicPartitionStoredAsSubDirsMerge(conf, path, tblDesc,
        aliases, partDesc);
    // set internal input format for all partition descriptors
    partDesc.setInputFileFormatClass(internalInputFormat);
  }

  /**
   * alter table ... concatenate
   * <br>
   * If it is skewed table, use subdirectories in inputpaths.
   */
  public void resolveConcatenateMerge(HiveConf conf) {
    isListBucketingAlterTableConcatenate =
        ((listBucketingCtx == null) ? false : listBucketingCtx
            .isSkewedStoredAsDir());
    LOG.info("isListBucketingAlterTableConcatenate : " +
        isListBucketingAlterTableConcatenate);
    if (isListBucketingAlterTableConcatenate) {
      // use sub-dir as inputpath.
      assert ((this.inputPaths != null) && (this.inputPaths.size() == 1)) :
          "alter table ... concatenate should only have one" +
              " directory inside inputpaths";
      Path dirPath = inputPaths.get(0);
      try {
        FileSystem inpFs = dirPath.getFileSystem(conf);
        List<FileStatus> status = HiveStatsUtils.getFileStatusRecurse(
            dirPath, listBucketingCtx.getSkewedColNames().size(), inpFs);
        List<Path> newInputPath = new ArrayList<Path>();
        boolean succeed = true;
        for (FileStatus s : status) {
          if (s.isDir()) {
            // Add the lb path to the list of input paths
            newInputPath.add(s.getPath());
          } else {
            // find file instead of dir. dont change inputpath
            succeed = false;
          }
        }
        assert (succeed || ((!succeed) && newInputPath.isEmpty())) :
            "This partition has "
                + " inconsistent file structure: "
                +
                "it is stored-as-subdir and expected all files in the same depth"
                + " of subdirectories.";
        if (succeed) {
          inputPaths.clear();
          inputPaths.addAll(newInputPath);
        }
      } catch (IOException e) {
        String msg =
            "Fail to get filesystem for directory name : " + dirPath.toUri();
        throw new RuntimeException(msg, e);
      }

    }
  }

  /**
   * @return the listBucketingCtx
   */
  public ListBucketingCtx getListBucketingCtx() {
    return listBucketingCtx;
  }

  /**
   * @param listBucketingCtx the listBucketingCtx to set
   */
  public void setListBucketingCtx(ListBucketingCtx listBucketingCtx) {
    this.listBucketingCtx = listBucketingCtx;
  }

  /**
   * @return the isListBucketingAlterTableConcatenate
   */
  public boolean isListBucketingAlterTableConcatenate() {
    return isListBucketingAlterTableConcatenate;
  }

  @Explain(displayName = "input format")
  public String getSourceTableInputFormat() {
    return srcTblInputFormat;
  }

  public void setSourceTableInputFormat(String srcTblInputFormat) {
    this.srcTblInputFormat = srcTblInputFormat;
  }

  @Explain(displayName = "merge level")
  public String getMergeLevel() {
    if (srcTblInputFormat != null) {
      if (srcTblInputFormat.equals(OrcInputFormat.class.getName())) {
        return "stripe";
      } else if (srcTblInputFormat.equals(RCFileInputFormat.class.getName())) {
        return "block";
      }
    }
    return null;
  }
}
