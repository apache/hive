package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelCollation;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin.MapJoinStreamingRelation;

import com.google.common.collect.ImmutableList;

public class HiveRelMdCollation {

  public static final RelMetadataProvider SOURCE =
          ChainedRelMetadataProvider.of(
                  ImmutableList.of(
                          ReflectiveRelMetadataProvider.reflectiveSource(
                                  BuiltInMethod.COLLATIONS.method, new HiveRelMdCollation()),
                          RelMdCollation.SOURCE));

  //~ Constructors -----------------------------------------------------------

  private HiveRelMdCollation() {}

  //~ Methods ----------------------------------------------------------------

  public ImmutableList<RelCollation> collations(HiveAggregate aggregate) {
    // Compute collations
    ImmutableList.Builder<RelFieldCollation> collationListBuilder =
            new ImmutableList.Builder<RelFieldCollation>();
    for (int pos : aggregate.getGroupSet().asList()) {
      final RelFieldCollation fieldCollation = new RelFieldCollation(pos);
      collationListBuilder.add(fieldCollation);
    }
    // Return aggregate collations
    return ImmutableList.of(
                RelCollationTraitDef.INSTANCE.canonize(
                        new HiveRelCollation(collationListBuilder.build())));
  }

  public ImmutableList<RelCollation> collations(HiveJoin join) {
    // Compute collations
    ImmutableList.Builder<RelFieldCollation> collationListBuilder =
            new ImmutableList.Builder<RelFieldCollation>();
    ImmutableList.Builder<RelFieldCollation> leftCollationListBuilder =
            new ImmutableList.Builder<RelFieldCollation>();
    ImmutableList.Builder<RelFieldCollation> rightCollationListBuilder =
            new ImmutableList.Builder<RelFieldCollation>();
    JoinPredicateInfo joinPredInfo =
            HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(join);
    for (int i = 0; i < joinPredInfo.getEquiJoinPredicateElements().size(); i++) {
      JoinLeafPredicateInfo joinLeafPredInfo = joinPredInfo.
          getEquiJoinPredicateElements().get(i);
      for (int leftPos : joinLeafPredInfo.getProjsFromLeftPartOfJoinKeysInJoinSchema()) {
        final RelFieldCollation leftFieldCollation = new RelFieldCollation(leftPos);
        collationListBuilder.add(leftFieldCollation);
        leftCollationListBuilder.add(leftFieldCollation);        
      }
      for (int rightPos : joinLeafPredInfo.getProjsFromRightPartOfJoinKeysInJoinSchema()) {
        final RelFieldCollation rightFieldCollation = new RelFieldCollation(rightPos);
        collationListBuilder.add(rightFieldCollation);
        rightCollationListBuilder.add(rightFieldCollation);        
      }
    }

    // Return join collations
    final ImmutableList<RelCollation> collation;
    switch (join.getJoinAlgorithm()) {
      case SMB_JOIN:
      case COMMON_JOIN:
        collation = ImmutableList.of(
                RelCollationTraitDef.INSTANCE.canonize(
                        new HiveRelCollation(collationListBuilder.build())));
        break;
      case BUCKET_JOIN:
      case MAP_JOIN:
        // Keep order from the streaming relation
        if (join.getMapJoinStreamingSide() == MapJoinStreamingRelation.LEFT_RELATION) {
          collation = ImmutableList.of(
                  RelCollationTraitDef.INSTANCE.canonize(
                          new HiveRelCollation(leftCollationListBuilder.build())));
        } else if (join.getMapJoinStreamingSide() == MapJoinStreamingRelation.RIGHT_RELATION) {
          collation = ImmutableList.of(
                  RelCollationTraitDef.INSTANCE.canonize(
                          new HiveRelCollation(rightCollationListBuilder.build())));
        } else {
          collation = null;
        }
        break;
      default:
        collation = null;
    }
    return collation;
  }

}
