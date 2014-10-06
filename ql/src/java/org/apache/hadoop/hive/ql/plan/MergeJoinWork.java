package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.mapred.JobConf;

public class MergeJoinWork extends BaseWork {

  private CommonMergeJoinOperator mergeJoinOp = null;
  private final List<BaseWork> mergeWorkList = new ArrayList<BaseWork>();
  private BaseWork bigTableWork;

  public MergeJoinWork() {
    super();
  }

  @Override
  public String getName() {
    return super.getName();
  }

  @Override
  public void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap) {
    getMainWork().replaceRoots(replacementMap);
  }

  @Override
  public Set<Operator<?>> getAllRootOperators() {
    return getMainWork().getAllRootOperators();
  }

  @Override
  public void configureJobConf(JobConf job) {
  }

  public CommonMergeJoinOperator getMergeJoinOperator() {
    return this.mergeJoinOp;
  }

  public void setMergeJoinOperator(CommonMergeJoinOperator mergeJoinOp) {
    this.mergeJoinOp = mergeJoinOp;
  }

  public void addMergedWork(BaseWork work, BaseWork connectWork) {
    if (work != null) {
      if ((bigTableWork != null) && (bigTableWork != work)) {
        assert false;
      }
      this.bigTableWork = work;
      setName(work.getName());
    }

    if (connectWork != null) {
      this.mergeWorkList.add(connectWork);
    }
  }

  @Explain(skipHeader=true, displayName = "Join")
  public List<BaseWork> getBaseWorkList() {
    return mergeWorkList;
  }

  public String getBigTableAlias() {
    return ((MapWork) bigTableWork).getAliasToWork().keySet().iterator().next();
  }

  @Explain(skipHeader=true, displayName = "Main")
  public BaseWork getMainWork() {
    return bigTableWork;
  }

  @Override
  public void setDummyOps(List<HashTableDummyOperator> dummyOps) {
    getMainWork().setDummyOps(dummyOps);
  }

  @Override
  public void addDummyOp(HashTableDummyOperator dummyOp) {
    getMainWork().addDummyOp(dummyOp);
  }
}
