package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;

import com.google.common.collect.ImmutableList;

public class HiveRelCollation extends RelCollationImpl {

  public HiveRelCollation(ImmutableList<RelFieldCollation> fieldCollations) {
    super(fieldCollations);
  }

}


