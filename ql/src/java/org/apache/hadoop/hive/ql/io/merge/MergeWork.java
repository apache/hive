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

package org.apache.hadoop.hive.ql.io.merge;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcFileMergeMapper;
import org.apache.hadoop.hive.ql.io.orc.OrcFileStripeMergeInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileBlockMergeInputFormat;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileMergeMapper;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Mapper;

@Explain(displayName = "Merge Work")
public class MergeWork extends MapWork implements Serializable {

  private static final long serialVersionUID = 1L;

  private transient List<Path> inputPaths;
  private transient Path outputDir;
  private boolean hasDynamicPartitions;
  private DynamicPartitionCtx dynPartCtx;
  private boolean isListBucketingAlterTableConcatenate;
  private ListBucketingCtx listBucketingCtx;
  private Class<? extends InputFormat> srcTblInputFormat;

  public MergeWork() {
  }

  public MergeWork(List<Path> inputPaths, Path outputDir,
      Class<? extends InputFormat> srcTblInputFormat) {
    this(inputPaths, outputDir, false, null, srcTblInputFormat);
  }

  public MergeWork(List<Path> inputPaths, Path outputDir,
      boolean hasDynamicPartitions, DynamicPartitionCtx dynPartCtx,
      Class<? extends InputFormat> srcTblInputFormat) {
    super();
    this.inputPaths = inputPaths;
    this.outputDir = outputDir;
    this.hasDynamicPartitions = hasDynamicPartitions;
    this.dynPartCtx = dynPartCtx;
    this.srcTblInputFormat = srcTblInputFormat;
    PartitionDesc partDesc = new PartitionDesc();
    if(srcTblInputFormat.equals(OrcInputFormat.class)) {
      partDesc.setInputFileFormatClass(OrcFileStripeMergeInputFormat.class);
    } else if(srcTblInputFormat.equals(RCFileInputFormat.class)) {
      partDesc.setInputFileFormatClass(RCFileBlockMergeInputFormat.class);
    }
    if(this.getPathToPartitionInfo() == null) {
      this.setPathToPartitionInfo(new LinkedHashMap<String, PartitionDesc>());
    }
    for(Path path: this.inputPaths) {
      this.getPathToPartitionInfo().put(path.toString(), partDesc);
    }
  }

  public List<Path> getInputPaths() {
    return inputPaths;
  }

  public void setInputPaths(List<Path> inputPaths) {
    this.inputPaths = inputPaths;
  }

  public Path getOutputDir() {
    return outputDir;
  }

  public void setOutputDir(Path outputDir) {
    this.outputDir = outputDir;
  }

  public Class<? extends Mapper> getMapperClass(Class<? extends InputFormat> klass) {
    if (klass.equals(RCFileInputFormat.class)) {
      return RCFileMergeMapper.class;
    } else if (klass.equals(OrcInputFormat.class)) {
      return OrcFileMergeMapper.class;
    }
    return null;
  }

  @Override
  public Long getMinSplitSize() {
    return null;
  }

  @Override
  public String getInputformat() {
    return CombineHiveInputFormat.class.getName();
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
  public void resolveDynamicPartitionStoredAsSubDirsMerge(HiveConf conf, Path path,
      TableDesc tblDesc, ArrayList<String> aliases, PartitionDesc partDesc) {

    String inputFormatClass = null;
    if (tblDesc.getInputFileFormatClass().equals(RCFileInputFormat.class)) {
      inputFormatClass = conf.getVar(HiveConf.ConfVars.HIVEMERGEINPUTFORMATBLOCKLEVEL);
    } else if (tblDesc.getInputFileFormatClass().equals(OrcInputFormat.class)){
      inputFormatClass = conf.getVar(HiveConf.ConfVars.HIVEMERGEINPUTFORMATSTRIPELEVEL);
    }

    try {
      partDesc.setInputFileFormatClass((Class <? extends InputFormat>)
          Class.forName(inputFormatClass));
    } catch (ClassNotFoundException e) {
      String msg = "Merge input format class not found";
      throw new RuntimeException(msg);
    }
    super.resolveDynamicPartitionStoredAsSubDirsMerge(conf, path, tblDesc, aliases, partDesc);

    // Add the DP path to the list of input paths
    inputPaths.add(path);
  }

  /**
   * alter table ... concatenate
   *
   * If it is skewed table, use subdirectories in inputpaths.
   */
  public void resolveConcatenateMerge(HiveConf conf) {
    isListBucketingAlterTableConcatenate = ((listBucketingCtx == null) ? false : listBucketingCtx
        .isSkewedStoredAsDir());
    if (isListBucketingAlterTableConcatenate) {
      // use sub-dir as inputpath.
      assert ((this.inputPaths != null) && (this.inputPaths.size() == 1)) :
        "alter table ... concatenate should only have one directory inside inputpaths";
      Path dirPath = inputPaths.get(0);
      try {
        FileSystem inpFs = dirPath.getFileSystem(conf);
        FileStatus[] status = HiveStatsUtils.getFileStatusRecurse(dirPath, listBucketingCtx
            .getSkewedColNames().size(), inpFs);
        List<Path> newInputPath = new ArrayList<Path>();
        boolean succeed = true;
        for (int i = 0; i < status.length; ++i) {
           if (status[i].isDir()) {
             // Add the lb path to the list of input paths
             newInputPath.add(status[i].getPath());
           } else {
             // find file instead of dir. dont change inputpath
             succeed = false;
           }
        }
        assert (succeed || ((!succeed) && newInputPath.isEmpty())) : "This partition has "
            + " inconsistent file structure: "
            + "it is stored-as-subdir and expected all files in the same depth of subdirectories.";
        if (succeed) {
          inputPaths.clear();
          inputPaths.addAll(newInputPath);
        }
      } catch (IOException e) {
        String msg = "Fail to get filesystem for directory name : " + dirPath.toUri();
        throw new RuntimeException(msg, e);
      }

    }
  }

  public DynamicPartitionCtx getDynPartCtx() {
    return dynPartCtx;
  }

  public void setDynPartCtx(DynamicPartitionCtx dynPartCtx) {
    this.dynPartCtx = dynPartCtx;
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

  public Class<? extends InputFormat> getSourceTableInputFormat() {
    return srcTblInputFormat;
  }

  @Explain(displayName = "input format")
  public String getStringifiedInputFormat() {
    return srcTblInputFormat.getCanonicalName();
  }

  @Explain(displayName = "merge level")
  public String getMergeLevel() {
    if (srcTblInputFormat != null) {
      if (srcTblInputFormat.equals(OrcInputFormat.class)) {
        return "stripe";
      } else if (srcTblInputFormat.equals(RCFileInputFormat.class)) {
        return "block";
      }
    }
    return null;
  }

  public void setSourceTableInputFormat(Class<? extends InputFormat> srcTblInputFormat) {
    this.srcTblInputFormat = srcTblInputFormat;
  }

}
