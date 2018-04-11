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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.SampleDesc;

/**
 * The transformation step that does sample pruning.
 *
 */
public class SamplePruner extends Transform {

  /**
   * SamplePrunerCtx.
   *
   */
  public static class SamplePrunerCtx implements NodeProcessorCtx {
    HashMap<TableScanOperator, SampleDesc> opToSamplePruner;

    public SamplePrunerCtx(
        HashMap<TableScanOperator, SampleDesc> opToSamplePruner) {
      this.opToSamplePruner = opToSamplePruner;
    }

    /**
     * @return the opToSamplePruner
     */
    public HashMap<TableScanOperator, SampleDesc> getOpToSamplePruner() {
      return opToSamplePruner;
    }

    /**
     * @param opToSamplePruner
     *          the opToSamplePruner to set
     */
    public void setOpToSamplePruner(
        HashMap<TableScanOperator, SampleDesc> opToSamplePruner) {
      this.opToSamplePruner = opToSamplePruner;
    }
  }

  // The log
  private static final Logger LOG = LoggerFactory
      .getLogger("hive.ql.optimizer.SamplePruner");

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.hive.ql.optimizer.Transform#transform(org.apache.hadoop
   * .hive.ql.parse.ParseContext)
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    // create a the context for walking operators
    SamplePrunerCtx samplePrunerCtx = new SamplePrunerCtx(pctx
        .getOpToSamplePruner());

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1",
      "(" + TableScanOperator.getOperatorName() + "%"
      + FilterOperator.getOperatorName() + "%"
      + FilterOperator.getOperatorName() + "%|"
      + TableScanOperator.getOperatorName() + "%"
      + FilterOperator.getOperatorName() + "%)"), getFilterProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
        samplePrunerCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  /**
   * FilterPPR filter processor.
   *
   */
  public static class FilterPPR implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator filOp = (FilterOperator) nd;
      FilterDesc filOpDesc = filOp.getConf();
      SampleDesc sampleDescr = filOpDesc.getSampleDescr();

      if ((sampleDescr == null) || !sampleDescr.getInputPruning()) {
        return null;
      }

      assert (stack.size() == 3 && stack.get(1) instanceof FilterOperator) ||
          stack.size() == 2;

      TableScanOperator tsOp = (TableScanOperator) stack.get(0);
      ((SamplePrunerCtx) procCtx).getOpToSamplePruner().put(tsOp, sampleDescr);
      return null;
    }
  }

  public static NodeProcessor getFilterProc() {
    return new FilterPPR();
  }

  /**
   * DefaultPPR default processor which does nothing.
   *
   */
  public static class DefaultPPR implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Nothing needs to be done.
      return null;
    }
  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultPPR();
  }

  /**
   * Prunes to get all the files in the partition that satisfy the TABLESAMPLE
   * clause.
   *
   * @param part
   *          The partition to prune
   * @return Path[]
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  public static Path[] prune(Partition part, SampleDesc sampleDescr)
      throws SemanticException {
    int num = sampleDescr.getNumerator();
    int den = sampleDescr.getDenominator();
    int bucketCount = part.getBucketCount();
    String fullScanMsg = "";

    // check if input pruning is possible
    // TODO: this code is buggy - it relies on having one file per bucket; no MM support (by design).
    boolean isMmTable = AcidUtils.isInsertOnlyTable(part.getTable().getParameters());
    if (sampleDescr.getInputPruning() && !isMmTable) {
      LOG.trace("numerator = " + num);
      LOG.trace("denominator = " + den);
      LOG.trace("bucket count = " + bucketCount);
      if (bucketCount == den) {
        Path[] ret = new Path[1];
        ret[0] = part.getBucketPath(num - 1);
        return (ret);
      } else if (bucketCount > den && bucketCount % den == 0) {
        int numPathsInSample = bucketCount / den;
        Path[] ret = new Path[numPathsInSample];
        for (int i = 0; i < numPathsInSample; i++) {
          ret[i] = part.getBucketPath(i * den + num - 1);
        }
        return ret;
      } else if (bucketCount < den && den % bucketCount == 0) {
        Path[] ret = new Path[1];
        ret[0] = part.getBucketPath((num - 1) % bucketCount);
        return ret;
      } else {
        // need to do full scan
        fullScanMsg = "Tablesample denominator " + den
            + " is not multiple/divisor of bucket count " + bucketCount
            + " of table " + part.getTable().getTableName();
      }
    } else {
      // need to do full scan
      fullScanMsg = isMmTable ? "MM table" : "Tablesample not on clustered columns";
    }
    LOG.warn(fullScanMsg + ", using full table scan");
    Path[] ret = part.getPath();
    return ret;
  }

  /**
   * Class used for return value of addPath()
   *
   */
  public static class AddPathReturnStatus {
    public AddPathReturnStatus(boolean hasFile, boolean allFile, long sizeLeft) {
      this.hasFile = hasFile;
      this.allFile = allFile;
      this.sizeLeft = sizeLeft;
    }
    // whether the sub-directory has any file
    public boolean hasFile;
    // whether all files are not sufficient to reach sizeLeft
    public boolean allFile;
    // remaining size needed after putting files in the return path list
    public long sizeLeft;
  }


  /**
   * Try to recursively add files in sub-directories into retPathList until
   * reaching the sizeLeft.
   * @param fs
   * @param pathPattern
   * @param sizeLeft
   * @param fileLimit
   * @param retPathList
   * @return status of the recursive call
   * @throws IOException
   */
  public static AddPathReturnStatus addPath(FileSystem fs, String pathPattern, long sizeLeft, int fileLimit,
      Collection<Path> retPathList)
      throws IOException {
    LOG.info("Path pattern = " + pathPattern);
    FileStatus srcs[] = fs.globStatus(new Path(pathPattern));
    Arrays.sort(srcs);

    boolean hasFile = false, allFile = true;

    for (FileStatus src : srcs) {
      if (sizeLeft <= 0) {
        allFile = false;
        break;
      }
      if (src.isDir()) {
        LOG.info("Got directory: " + src.getPath());
        AddPathReturnStatus ret = addPath(fs, src.getPath().toString() + "/*", sizeLeft,
            fileLimit, retPathList);
        if (ret == null) {
          // not qualify this optimization
          return null;
        }
        sizeLeft = ret.sizeLeft;
        hasFile |= ret.hasFile;
        allFile &= ret.allFile;
      } else {
        LOG.info("Got file: " + src.getPath());
        hasFile = true;
        retPathList.add(src.getPath());
        sizeLeft -= src.getLen();
        if (retPathList.size() >= fileLimit && sizeLeft > 0) {
          return null;
        }
      }
    }
    return new AddPathReturnStatus(hasFile, allFile, sizeLeft);
  }

  public enum LimitPruneRetStatus {
    // no files in the partition
    NoFile,
    // sum size of all files in the partition is smaller than size required
    NeedAllFiles,
    // a susbset of files for the partition are sufficient for the optimization
    NeedSomeFiles,
    // the partition doesn't qualify the global limit optimization for some reason
    NotQualify
  }


  /**
   * Try to generate a list of subset of files in the partition to reach a size
   * limit with number of files less than fileLimit
   * @param part
   * @param sizeLimit
   * @param fileLimit
   * @param retPathList list of Paths returned
   * @return the result of the attempt
   * @throws SemanticException
   */
  public static LimitPruneRetStatus limitPrune(Partition part, long sizeLimit, int fileLimit,
      Collection<Path> retPathList)
      throws SemanticException {

    try {
      FileSystem fs = part.getDataLocation().getFileSystem(Hive.get().getConf());
      String pathPattern = part.getDataLocation().toString() + "/*";
      AddPathReturnStatus ret = addPath(fs, pathPattern, sizeLimit, fileLimit, retPathList);
      if (ret == null) {
        return LimitPruneRetStatus.NotQualify;
      } else if (!ret.hasFile){
        return LimitPruneRetStatus.NoFile;
      } else if (ret.sizeLeft > 0) {
        return LimitPruneRetStatus.NotQualify;
      } else if (ret.allFile) {
        return LimitPruneRetStatus.NeedAllFiles;
      } else {
        return LimitPruneRetStatus.NeedSomeFiles;
      }
    } catch (Exception e) {
      throw new RuntimeException("Cannot get path", e);
    }

  }

}
