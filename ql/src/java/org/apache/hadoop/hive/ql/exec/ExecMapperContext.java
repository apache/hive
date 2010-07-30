package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.IOContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork.BucketMapJoinContext;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

public class ExecMapperContext {

  public static final Log l4j = ExecMapper.l4j;

  // lastInputFile should be changed by the root of the operator tree ExecMapper.map()
  // but kept unchanged throughout the operator tree for one row
  private String lastInputFile = null;

  // currentInputFile will be updated only by inputFileChanged(). If inputFileChanged()
  // is not called throughout the opertor tree, currentInputFile won't be used anyways
  // so it won't be updated.
  private String currentInputFile = null;
  private boolean inputFileChecked = false;
  private Integer fileId = new Integer(-1);
  private MapredLocalWork localWork = null;
  private Map<String, FetchOperator> fetchOperators;
  private JobConf jc;

  private IOContext ioCxt;
  
  public ExecMapperContext() {
    ioCxt = IOContext.get();
  }

  public void processInputFileChangeForLocalWork() throws HiveException {
    // put inputFileChanged() after localWork check
    if (this.localWork != null && inputFileChanged()) {
      processMapLocalWork(localWork.getInputFileChangeSensitive());
    }
  }


  /**
   * For CompbineFileInputFormat, the mapper's input file will be changed on the
   * fly, and the input file name is passed to jobConf by shims/initNextRecordReader.
   * If the map local work has any mapping depending on the current
   * mapper's input file, the work need to clear context and re-initialization
   * after the input file changed. This is first introduced to process bucket
   * map join.
   *
   * @return
   */
  public boolean inputFileChanged() {
    if (!inputFileChecked) {
      currentInputFile = this.ioCxt.getInputFile();
      inputFileChecked = true;
    }
    return lastInputFile == null || !lastInputFile.equals(currentInputFile);
  }

  /**
   * Reset the execution context for each new row. This function should be called only
   * once at the root of the operator tree -- ExecMapper.map().
   * Note: this function should be kept minimum since it is called for each input row.
   */
  public void resetRow() {
    // Update the lastInputFile with the currentInputFile.
    lastInputFile = currentInputFile;
    inputFileChecked = false;
  }

  public String getLastInputFile() {
    return lastInputFile;
  }

  public void setLastInputFile(String lastInputFile) {
    this.lastInputFile = lastInputFile;
  }

  private void processMapLocalWork(boolean inputFileChangeSenstive) throws HiveException {
    // process map local operators
    if (fetchOperators != null) {
      try {
        int fetchOpNum = 0;
        for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
          int fetchOpRows = 0;
          String alias = entry.getKey();
          FetchOperator fetchOp = entry.getValue();

          if (inputFileChangeSenstive) {
            fetchOp.clearFetchContext();
            setUpFetchOpContext(fetchOp, alias);
          }

          Operator<? extends Serializable> forwardOp = localWork
              .getAliasToWork().get(alias);

          while (true) {
            InspectableObject row = fetchOp.getNextRow();
            if (row == null) {
              forwardOp.close(false);
              break;
            }
            fetchOpRows++;
            forwardOp.process(row.o, 0);
            // check if any operator had a fatal error or early exit during
            // execution
            if (forwardOp.getDone()) {
              ExecMapper.setDone(true);
              break;
            }
          }

          if (l4j.isInfoEnabled()) {
            l4j.info("fetch " + fetchOpNum++ + " processed " + fetchOpRows
                + " used mem: "
                + ExecMapper.memoryMXBean.getHeapMemoryUsage().getUsed());
          }
        }
      } catch (Throwable e) {
        if (e instanceof OutOfMemoryError) {
          // Don't create a new object if we are already out of memory
          throw (OutOfMemoryError) e;
        } else {
          throw new HiveException(
              "Hive Runtime Error: Map local work failed", e);
        }
      }
    }
  }

  private void setUpFetchOpContext(FetchOperator fetchOp, String alias)
      throws Exception {
    String currentInputFile = HiveConf.getVar(jc,
        HiveConf.ConfVars.HADOOPMAPFILENAME);
    BucketMapJoinContext bucketMatcherCxt = this.localWork
        .getBucketMapjoinContext();
    Class<? extends BucketMatcher> bucketMatcherCls = bucketMatcherCxt
        .getBucketMatcherClass();
    BucketMatcher bucketMatcher = (BucketMatcher) ReflectionUtils.newInstance(
        bucketMatcherCls, null);
    bucketMatcher.setAliasBucketFileNameMapping(bucketMatcherCxt
        .getAliasBucketFileNameMapping());

    List<Path> aliasFiles = bucketMatcher.getAliasBucketFiles(currentInputFile,
        bucketMatcherCxt.getMapJoinBigTableAlias(), alias);
    Iterator<Path> iter = aliasFiles.iterator();
    fetchOp.setupContext(iter, null);
  }

  public String getCurrentInputFile() {
    return currentInputFile;
  }

  public void setCurrentInputFile(String currentInputFile) {
    this.currentInputFile = currentInputFile;
  }

  public JobConf getJc() {
    return jc;
  }
  public void setJc(JobConf jc) {
    this.jc = jc;
  }

  public MapredLocalWork getLocalWork() {
    return localWork;
  }

  public void setLocalWork(MapredLocalWork localWork) {
    this.localWork = localWork;
  }

  public Integer getFileId() {
    return fileId;
  }

  public void setFileId(Integer fileId) {
    this.fileId = fileId;
  }

  public Map<String, FetchOperator> getFetchOperators() {
    return fetchOperators;
  }

  public void setFetchOperators(Map<String, FetchOperator> fetchOperators) {
    this.fetchOperators = fetchOperators;
  }
  
  public IOContext getIoCxt() {
    return ioCxt;
  }

  public void setIoCxt(IOContext ioCxt) {
    this.ioCxt = ioCxt;
  }

}
