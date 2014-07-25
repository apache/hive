package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import net.hydromatic.linq4j.Ord;
import net.hydromatic.optiq.util.BitSets;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveAggregateRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveSortRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRel;
import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SetOpRel;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.TableFunctionRel;
import org.eigenbase.rel.TableModificationRel;
import org.eigenbase.rel.ValuesRel;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataTypeImpl;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexPermuteInputsShuttle;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.rex.RexVisitor;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.util.Bug;
import org.eigenbase.util.Pair;
import org.eigenbase.util.ReflectUtil;
import org.eigenbase.util.ReflectiveVisitor;
import org.eigenbase.util.Util;
import org.eigenbase.util.mapping.IntPair;
import org.eigenbase.util.mapping.Mapping;
import org.eigenbase.util.mapping.MappingType;
import org.eigenbase.util.mapping.Mappings;

/**
 * Transformer that walks over a tree of relational expressions, replacing each
 * {@link RelNode} with a 'slimmed down' relational expression that projects
 * only the columns required by its consumer.
 *
 * <p>
 * Uses multi-methods to fire the right rule for each type of relational
 * expression. This allows the transformer to be extended without having to add
 * a new method to RelNode, and without requiring a collection of rule classes
 * scattered to the four winds.
 *
 * <p>
 * REVIEW: jhyde, 2009/7/28: Is sql2rel the correct package for this class?
 * Trimming fields is not an essential part of SQL-to-Rel translation, and
 * arguably belongs in the optimization phase. But this transformer does not
 * obey the usual pattern for planner rules; it is difficult to do so, because
 * each {@link RelNode} needs to return a different set of fields after
 * trimming.
 *
 * <p>
 * TODO: Change 2nd arg of the {@link #trimFields} method from BitSet to
 * Mapping. Sometimes it helps the consumer if you return the columns in a
 * particular order. For instance, it may avoid a project at the top of the tree
 * just for reordering. Could ease the transition by writing methods that
 * convert BitSet to Mapping and vice versa.
 */
public class HiveRelFieldTrimmer implements ReflectiveVisitor {
  // ~ Static fields/initializers ---------------------------------------------

  // ~ Instance fields --------------------------------------------------------

  private final ReflectUtil.MethodDispatcher<TrimResult> trimFieldsDispatcher;

  // ~ Constructors -----------------------------------------------------------

  /**
   * Creates a RelFieldTrimmer.
   *
   * @param validator
   *          Validator
   */
  public HiveRelFieldTrimmer(SqlValidator validator) {
    Util.discard(validator); // may be useful one day
    this.trimFieldsDispatcher = ReflectUtil.createMethodDispatcher(
        TrimResult.class, this, "trimFields", RelNode.class, BitSet.class,
        Set.class);
  }

  // ~ Methods ----------------------------------------------------------------

  /**
   * Trims unused fields from a relational expression.
   *
   * <p>
   * We presume that all fields of the relational expression are wanted by its
   * consumer, so only trim fields that are not used within the tree.
   *
   * @param root
   *          Root node of relational expression
   * @return Trimmed relational expression
   */
  public RelNode trim(RelNode root) {
    final int fieldCount = root.getRowType().getFieldCount();
    final BitSet fieldsUsed = BitSets.range(fieldCount);
    final Set<RelDataTypeField> extraFields = Collections.emptySet();
    final TrimResult trimResult = dispatchTrimFields(root, fieldsUsed,
        extraFields);
    if (!trimResult.right.isIdentity()) {
      throw new IllegalArgumentException();
    }
    return trimResult.left;
  }

  /**
   * Trims the fields of an input relational expression.
   *
   * @param rel
   *          Relational expression
   * @param input
   *          Input relational expression, whose fields to trim
   * @param fieldsUsed
   *          Bitmap of fields needed by the consumer
   * @return New relational expression and its field mapping
   */
  protected TrimResult trimChild(RelNode rel, RelNode input, BitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    Util.discard(rel);
    if (input.getClass().getName().endsWith("MedMdrClassExtentRel")) {
      // MedMdrJoinRule cannot handle Join of Project of
      // MedMdrClassExtentRel, only naked MedMdrClassExtentRel.
      // So, disable trimming.
      fieldsUsed = BitSets.range(input.getRowType().getFieldCount());
    }
    return dispatchTrimFields(input, fieldsUsed, extraFields);
  }

  /**
   * Trims a child relational expression, then adds back a dummy project to
   * restore the fields that were removed.
   *
   * <p>
   * Sounds pointless? It causes unused fields to be removed further down the
   * tree (towards the leaves), but it ensure that the consuming relational
   * expression continues to see the same fields.
   *
   * @param rel
   *          Relational expression
   * @param input
   *          Input relational expression, whose fields to trim
   * @param fieldsUsed
   *          Bitmap of fields needed by the consumer
   * @return New relational expression and its field mapping
   */
  protected TrimResult trimChildRestore(RelNode rel, RelNode input,
      BitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    TrimResult trimResult = trimChild(rel, input, fieldsUsed, extraFields);
    if (trimResult.right.isIdentity()) {
      return trimResult;
    }
    final RelDataType rowType = input.getRowType();
    List<RelDataTypeField> fieldList = rowType.getFieldList();
    final List<RexNode> exprList = new ArrayList<RexNode>();
    final List<String> nameList = rowType.getFieldNames();
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    assert trimResult.right.getSourceCount() == fieldList.size();
    for (int i = 0; i < fieldList.size(); i++) {
      int source = trimResult.right.getTargetOpt(i);
      RelDataTypeField field = fieldList.get(i);
      exprList.add(source < 0 ? rexBuilder.makeZeroLiteral(field.getType())
          : rexBuilder.makeInputRef(field.getType(), source));
    }
    RelNode project = CalcRel
        .createProject(trimResult.left, exprList, nameList);
    return new TrimResult(project, Mappings.createIdentity(fieldList.size()));
  }

  /**
   * Invokes {@link #trimFields}, or the appropriate method for the type of the
   * rel parameter, using multi-method dispatch.
   *
   * @param rel
   *          Relational expression
   * @param fieldsUsed
   *          Bitmap of fields needed by the consumer
   * @return New relational expression and its field mapping
   */
  protected final TrimResult dispatchTrimFields(RelNode rel, BitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final TrimResult trimResult = trimFieldsDispatcher.invoke(rel, fieldsUsed,
        extraFields);
    final RelNode newRel = trimResult.left;
    final Mapping mapping = trimResult.right;
    final int fieldCount = rel.getRowType().getFieldCount();
    assert mapping.getSourceCount() == fieldCount : "source: "
        + mapping.getSourceCount() + " != " + fieldCount;
    final int newFieldCount = newRel.getRowType().getFieldCount();
    assert mapping.getTargetCount() + extraFields.size() == newFieldCount : "target: "
        + mapping.getTargetCount()
        + " + "
        + extraFields.size()
        + " != "
        + newFieldCount;
    if (Bug.TODO_FIXED) {
      assert newFieldCount > 0 : "rel has no fields after trim: " + rel;
    }
    if (newRel.equals(rel)) {
      return new TrimResult(rel, mapping);
    }
    return trimResult;
  }

  /**
   * Visit method, per {@link org.eigenbase.util.ReflectiveVisitor}.
   *
   * <p>
   * This method is invoked reflectively, so there may not be any apparent calls
   * to it. The class (or derived classes) may contain overloads of this method
   * with more specific types for the {@code rel} parameter.
   *
   * <p>
   * Returns a pair: the relational expression created, and the mapping between
   * the original fields and the fields of the newly created relational
   * expression.
   *
   * @param rel
   *          Relational expression
   * @param fieldsUsed
   *          Fields needed by the consumer
   * @return relational expression and mapping
   */
  public TrimResult trimFields(HiveRel rel, BitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    // We don't know how to trim this kind of relational expression, so give
    // it back intact.
    Util.discard(fieldsUsed);
    return new TrimResult(rel, Mappings.createIdentity(rel.getRowType()
        .getFieldCount()));
  }

  /**
   * Variant of {@link #trimFields(RelNode, BitSet, Set)} for
   * {@link HiveProjectRel} .
   */
  public TrimResult trimFields(HiveProjectRel project, BitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = project.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelNode input = project.getChild();
    final RelDataType inputRowType = input.getRowType();

    // Which fields are required from the input?
    BitSet inputFieldsUsed = new BitSet(inputRowType.getFieldCount());
    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<RelDataTypeField>(
        extraFields);
    RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(
        inputFieldsUsed, inputExtraFields);
    for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
      if (fieldsUsed.get(ord.i)) {
        ord.e.accept(inputFinder);
      }
    }

    // Create input with trimmed columns.
    TrimResult trimResult = trimChild(project, input, inputFieldsUsed,
        inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input && fieldsUsed.cardinality() == fieldCount) {
      return new TrimResult(project, Mappings.createIdentity(fieldCount));
    }

    // Some parts of the system can't handle rows with zero fields, so
    // pretend that one field is used.
    if (fieldsUsed.cardinality() == 0) {
      final Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION,
          fieldCount, 1);
      final RexLiteral expr = project.getCluster().getRexBuilder()
          .makeExactLiteral(BigDecimal.ZERO);
      RelDataType newRowType = project
          .getCluster()
          .getTypeFactory()
          .createStructType(Collections.singletonList(expr.getType()),
              Collections.singletonList("DUMMY"));
      HiveProjectRel newProject = new HiveProjectRel(project.getCluster(),
          project.getCluster().traitSetOf(HiveRel.CONVENTION), newInput,
          Collections.<RexNode> singletonList(expr), newRowType,
          project.getFlags());
      return new TrimResult(newProject, mapping);
    }

    // Build new project expressions, and populate the mapping.
    List<RexNode> newProjectExprList = new ArrayList<RexNode>();
    final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(
        inputMapping, newInput);
    final Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION,
        fieldCount, fieldsUsed.cardinality());
    for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
      if (fieldsUsed.get(ord.i)) {
        mapping.set(ord.i, newProjectExprList.size());
        RexNode newProjectExpr = ord.e.accept(shuttle);
        newProjectExprList.add(newProjectExpr);
      }
    }

    final RelDataType newRowType = project.getCluster().getTypeFactory()
        .createStructType(Mappings.apply3(mapping, rowType.getFieldList()));

    final List<RelCollation> newCollations = RexUtil.apply(inputMapping,
        project.getCollationList());

    final RelNode newProject;
    if (RemoveTrivialProjectRule.isIdentity(newProjectExprList, newRowType,
        newInput.getRowType())) {
      // The new project would be the identity. It is equivalent to return
      // its child.
      newProject = newInput;
    } else {
      newProject = new HiveProjectRel(project.getCluster(), project
          .getCluster()
          .traitSetOf(
              newCollations.isEmpty() ? HiveRel.CONVENTION : newCollations
                  .get(0)), newInput, newProjectExprList, newRowType,
          project.getFlags());
      assert newProject.getClass() == project.getClass();
    }
    return new TrimResult(newProject, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, BitSet, Set)} for
   * {@link HiveFilterRel}.
   */
  public TrimResult trimFields(HiveFilterRel filter, BitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = filter.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RexNode conditionExpr = filter.getCondition();
    final RelNode input = filter.getChild();

    // We use the fields used by the consumer, plus any fields used in the
    // filter.
    BitSet inputFieldsUsed = (BitSet) fieldsUsed.clone();
    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<RelDataTypeField>(
        extraFields);
    RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(
        inputFieldsUsed, inputExtraFields);
    conditionExpr.accept(inputFinder);

    // Create input with trimmed columns.
    TrimResult trimResult = trimChild(filter, input, inputFieldsUsed,
        inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input && fieldsUsed.cardinality() == fieldCount) {
      return new TrimResult(filter, Mappings.createIdentity(fieldCount));
    }

    // Build new project expressions, and populate the mapping.
    final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(
        inputMapping, newInput);
    RexNode newConditionExpr = conditionExpr.accept(shuttle);

    final HiveFilterRel newFilter = new HiveFilterRel(filter.getCluster(),
        filter.getCluster().traitSetOf(HiveRel.CONVENTION), newInput,
        newConditionExpr);
    assert newFilter.getClass() == filter.getClass();

    // The result has the same mapping as the input gave us. Sometimes we
    // return fields that the consumer didn't ask for, because the filter
    // needs them for its condition.
    return new TrimResult(newFilter, inputMapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, BitSet, Set)} for {@link SortRel}.
   */
  public TrimResult trimFields(HiveSortRel sort, BitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = sort.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelCollation collation = sort.getCollation();
    final RelNode input = sort.getChild();

    // We use the fields used by the consumer, plus any fields used as sort
    // keys.
    BitSet inputFieldsUsed = (BitSet) fieldsUsed.clone();
    for (RelFieldCollation field : collation.getFieldCollations()) {
      inputFieldsUsed.set(field.getFieldIndex());
    }

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult = trimChild(sort, input, inputFieldsUsed,
        inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input && inputMapping.isIdentity()
        && fieldsUsed.cardinality() == fieldCount) {
      return new TrimResult(sort, Mappings.createIdentity(fieldCount));
    }

    final SortRel newSort = sort.copy(sort.getTraitSet(), newInput,
        RexUtil.apply(inputMapping, collation));
    assert newSort.getClass() == sort.getClass();

    // The result has the same mapping as the input gave us. Sometimes we
    // return fields that the consumer didn't ask for, because the filter
    // needs them for its condition.
    return new TrimResult(newSort, inputMapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, BitSet, Set)} for {@link JoinRel}.
   */
  public TrimResult trimFields(HiveJoinRel join, BitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = join.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RexNode conditionExpr = join.getCondition();
    final int systemFieldCount = join.getSystemFieldList().size();

    /*
     * todo: hb 6/26/14 for left SemiJoin we cannot trim yet.
     * HiveJoinRelNode.deriveRowType return only left columns. Default field
     * trimmer needs to be enhanced to handle this.
     */
    if (join.isLeftSemiJoin()) {
      return new TrimResult(join, Mappings.createIdentity(fieldCount));
    }

    // Add in fields used in the condition.
    BitSet fieldsUsedPlus = (BitSet) fieldsUsed.clone();
    final Set<RelDataTypeField> combinedInputExtraFields = new LinkedHashSet<RelDataTypeField>(
        extraFields);
    RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(
        fieldsUsedPlus, combinedInputExtraFields);
    conditionExpr.accept(inputFinder);

    // If no system fields are used, we can remove them.
    int systemFieldUsedCount = 0;
    for (int i = 0; i < systemFieldCount; ++i) {
      if (fieldsUsed.get(i)) {
        ++systemFieldUsedCount;
      }
    }
    final int newSystemFieldCount;
    if (systemFieldUsedCount == 0) {
      newSystemFieldCount = 0;
    } else {
      newSystemFieldCount = systemFieldCount;
    }

    int offset = systemFieldCount;
    int changeCount = 0;
    int newFieldCount = newSystemFieldCount;
    List<RelNode> newInputs = new ArrayList<RelNode>(2);
    List<Mapping> inputMappings = new ArrayList<Mapping>();
    List<Integer> inputExtraFieldCounts = new ArrayList<Integer>();
    for (RelNode input : join.getInputs()) {
      final RelDataType inputRowType = input.getRowType();
      final int inputFieldCount = inputRowType.getFieldCount();

      // Compute required mapping.
      BitSet inputFieldsUsed = new BitSet(inputFieldCount);
      for (int bit : BitSets.toIter(fieldsUsedPlus)) {
        if (bit >= offset && bit < offset + inputFieldCount) {
          inputFieldsUsed.set(bit - offset);
        }
      }

      // If there are system fields, we automatically use the
      // corresponding field in each input.
      if (newSystemFieldCount > 0) {
        // calling with newSystemFieldCount == 0 should be safe but hits
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6222207
        inputFieldsUsed.set(0, newSystemFieldCount);
      }

      // FIXME: We ought to collect extra fields for each input
      // individually. For now, we assume that just one input has
      // on-demand fields.
      Set<RelDataTypeField> inputExtraFields = RelDataTypeImpl.extra(rowType) == null ? Collections
          .<RelDataTypeField> emptySet() : combinedInputExtraFields;
      inputExtraFieldCounts.add(inputExtraFields.size());
      TrimResult trimResult = trimChild(join, input, inputFieldsUsed,
          inputExtraFields);
      newInputs.add(trimResult.left);
      if (trimResult.left != input) {
        ++changeCount;
      }

      final Mapping inputMapping = trimResult.right;
      inputMappings.add(inputMapping);

      // Move offset to point to start of next input.
      offset += inputFieldCount;
      newFieldCount += inputMapping.getTargetCount() + inputExtraFields.size();
    }

    Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION,
        fieldCount, newFieldCount);
    for (int i = 0; i < newSystemFieldCount; ++i) {
      mapping.set(i, i);
    }
    offset = systemFieldCount;
    int newOffset = newSystemFieldCount;
    for (int i = 0; i < inputMappings.size(); i++) {
      Mapping inputMapping = inputMappings.get(i);
      for (IntPair pair : inputMapping) {
        mapping.set(pair.source + offset, pair.target + newOffset);
      }
      offset += inputMapping.getSourceCount();
      newOffset += inputMapping.getTargetCount() + inputExtraFieldCounts.get(i);
    }

    if (changeCount == 0 && mapping.isIdentity()) {
      return new TrimResult(join, Mappings.createIdentity(fieldCount));
    }

    // Build new join.
    final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(mapping,
        newInputs.get(0), newInputs.get(1));
    RexNode newConditionExpr = conditionExpr.accept(shuttle);

    final HiveJoinRel newJoin = join.copy(join.getTraitSet(), newConditionExpr,
        newInputs.get(0), newInputs.get(1), join.getJoinType(), false);

    return new TrimResult(newJoin, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, BitSet, Set)} for {@link SetOpRel}
   * (including UNION and UNION ALL).
   */
  public TrimResult trimFields(SetOpRel setOp, BitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = setOp.getRowType();
    final int fieldCount = rowType.getFieldCount();
    int changeCount = 0;

    // Fennel abhors an empty row type, so pretend that the parent rel
    // wants the last field. (The last field is the least likely to be a
    // system field.)
    if (fieldsUsed.isEmpty()) {
      fieldsUsed.set(rowType.getFieldCount() - 1);
    }

    // Compute the desired field mapping. Give the consumer the fields they
    // want, in the order that they appear in the bitset.
    final Mapping mapping = createMapping(fieldsUsed, fieldCount);

    // Create input with trimmed columns.
    final List<RelNode> newInputs = new ArrayList<RelNode>();
    for (RelNode input : setOp.getInputs()) {
      TrimResult trimResult = trimChild(setOp, input, fieldsUsed, extraFields);
      RelNode newInput = trimResult.left;
      final Mapping inputMapping = trimResult.right;

      // We want "mapping", the input gave us "inputMapping", compute
      // "remaining" mapping.
      // | | |
      // |---------------- mapping ---------->|
      // |-- inputMapping -->| |
      // | |-- remaining -->|
      //
      // For instance, suppose we have columns [a, b, c, d],
      // the consumer asked for mapping = [b, d],
      // and the transformed input has columns inputMapping = [d, a, b].
      // remaining will permute [b, d] to [d, a, b].
      Mapping remaining = Mappings.divide(mapping, inputMapping);

      // Create a projection; does nothing if remaining is identity.
      newInput = CalcRel.projectMapping(newInput, remaining, null);

      if (input != newInput) {
        ++changeCount;
      }
      newInputs.add(newInput);
    }

    // If the input is unchanged, and we need to project all columns,
    // there's to do.
    if (changeCount == 0 && mapping.isIdentity()) {
      return new TrimResult(setOp, mapping);
    }

    RelNode newSetOp = setOp.copy(setOp.getTraitSet(), newInputs);
    return new TrimResult(newSetOp, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, BitSet, Set)} for
   * {@link AggregateRel}.
   *
   * @throws InvalidRelException
   */
  public TrimResult trimFields(HiveAggregateRel aggregate, BitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) throws InvalidRelException {
    // Fields:
    //
    // | sys fields | group fields | agg functions |
    //
    // Two kinds of trimming:
    //
    // 1. If agg rel has system fields but none of these are used, create an
    // agg rel with no system fields.
    //
    // 2. If aggregate functions are not used, remove them.
    //
    // But grouping fields stay, even if they are not used.

    final RelDataType rowType = aggregate.getRowType();

    // Compute which input fields are used.
    BitSet inputFieldsUsed = new BitSet();
    // 1. group fields are always used
    for (int i : BitSets.toIter(aggregate.getGroupSet())) {
      inputFieldsUsed.set(i);
    }
    // 2. agg functions
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      for (int i : aggCall.getArgList()) {
        inputFieldsUsed.set(i);
      }
    }

    // Create input with trimmed columns.
    final RelNode input = aggregate.getInput(0);
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    final TrimResult trimResult = trimChild(aggregate, input, inputFieldsUsed,
        inputExtraFields);
    final RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing to do.
    if (input == newInput
        && fieldsUsed.equals(BitSets.range(rowType.getFieldCount()))) {
      return new TrimResult(aggregate, Mappings.createIdentity(rowType
          .getFieldCount()));
    }

    // Which agg calls are used by our consumer?
    final int groupCount = aggregate.getGroupSet().cardinality();
    int j = groupCount;
    int usedAggCallCount = 0;
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      if (fieldsUsed.get(j++)) {
        ++usedAggCallCount;
      }
    }

    // Offset due to the number of system fields having changed.
    Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION,
        rowType.getFieldCount(), groupCount + usedAggCallCount);

    final BitSet newGroupSet = Mappings.apply(inputMapping,
        aggregate.getGroupSet());

    // Populate mapping of where to find the fields. System and grouping
    // fields first.
    for (IntPair pair : inputMapping) {
      if (pair.source < groupCount) {
        mapping.set(pair.source, pair.target);
      }
    }

    // Now create new agg calls, and populate mapping for them.
    final List<AggregateCall> newAggCallList = new ArrayList<AggregateCall>();
    j = groupCount;
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (fieldsUsed.get(j)) {
        AggregateCall newAggCall = new AggregateCall(aggCall.getAggregation(),
            aggCall.isDistinct(), Mappings.apply2(inputMapping,
                aggCall.getArgList()), aggCall.getType(), aggCall.getName());
        if (newAggCall.equals(aggCall)) {
          newAggCall = aggCall; // immutable -> canonize to save space
        }
        mapping.set(j, groupCount + newAggCallList.size());
        newAggCallList.add(newAggCall);
      }
      ++j;
    }

    RelNode newAggregate = new HiveAggregateRel(aggregate.getCluster(),
        aggregate.getCluster().traitSetOf(HiveRel.CONVENTION), newInput,
        newGroupSet, newAggCallList);

    assert newAggregate.getClass() == aggregate.getClass();

    return new TrimResult(newAggregate, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, BitSet, Set)} for
   * {@link TableModificationRel}.
   */
  public TrimResult trimFields(TableModificationRel modifier,
      BitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    // Ignore what consumer wants. We always project all columns.
    Util.discard(fieldsUsed);

    final RelDataType rowType = modifier.getRowType();
    final int fieldCount = rowType.getFieldCount();
    RelNode input = modifier.getChild();

    // We want all fields from the child.
    final int inputFieldCount = input.getRowType().getFieldCount();
    BitSet inputFieldsUsed = BitSets.range(inputFieldCount);

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult = trimChild(modifier, input, inputFieldsUsed,
        inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;
    if (!inputMapping.isIdentity()) {
      // We asked for all fields. Can't believe that the child decided
      // to permute them!
      throw Util.newInternal("Expected identity mapping, got " + inputMapping);
    }

    TableModificationRel newModifier = modifier;
    if (newInput != input) {
      newModifier = modifier.copy(modifier.getTraitSet(),
          Collections.singletonList(newInput));
    }
    assert newModifier.getClass() == modifier.getClass();

    // Always project all fields.
    Mapping mapping = Mappings.createIdentity(fieldCount);
    return new TrimResult(newModifier, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, BitSet, Set)} for
   * {@link TableFunctionRel}.
   */
  public TrimResult trimFields(TableFunctionRel tabFun, BitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = tabFun.getRowType();
    final int fieldCount = rowType.getFieldCount();
    List<RelNode> newInputs = new ArrayList<RelNode>();

    for (RelNode input : tabFun.getInputs()) {
      final int inputFieldCount = input.getRowType().getFieldCount();
      BitSet inputFieldsUsed = BitSets.range(inputFieldCount);

      // Create input with trimmed columns.
      final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
      TrimResult trimResult = trimChildRestore(tabFun, input, inputFieldsUsed,
          inputExtraFields);
      assert trimResult.right.isIdentity();
      newInputs.add(trimResult.left);
    }

    TableFunctionRel newTabFun = tabFun;
    if (!tabFun.getInputs().equals(newInputs)) {
      newTabFun = tabFun.copy(tabFun.getTraitSet(), newInputs);
    }
    assert newTabFun.getClass() == tabFun.getClass();

    // Always project all fields.
    Mapping mapping = Mappings.createIdentity(fieldCount);
    return new TrimResult(newTabFun, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, BitSet, Set)} for
   * {@link org.eigenbase.rel.ValuesRel}.
   */
  public TrimResult trimFields(ValuesRel values, BitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = values.getRowType();
    final int fieldCount = rowType.getFieldCount();

    // If they are asking for no fields, we can't give them what they want,
    // because zero-column records are illegal. Give them the last field,
    // which is unlikely to be a system field.
    if (fieldsUsed.isEmpty()) {
      fieldsUsed = BitSets.range(fieldCount - 1, fieldCount);
    }

    // If all fields are used, return unchanged.
    if (fieldsUsed.equals(BitSets.range(fieldCount))) {
      Mapping mapping = Mappings.createIdentity(fieldCount);
      return new TrimResult(values, mapping);
    }

    List<List<RexLiteral>> newTuples = new ArrayList<List<RexLiteral>>();
    for (List<RexLiteral> tuple : values.getTuples()) {
      List<RexLiteral> newTuple = new ArrayList<RexLiteral>();
      for (int field : BitSets.toIter(fieldsUsed)) {
        newTuple.add(tuple.get(field));
      }
      newTuples.add(newTuple);
    }

    final Mapping mapping = createMapping(fieldsUsed, fieldCount);
    RelDataType newRowType = values.getCluster().getTypeFactory()
        .createStructType(Mappings.apply3(mapping, rowType.getFieldList()));
    final ValuesRel newValues = new ValuesRel(values.getCluster(), newRowType,
        newTuples);
    return new TrimResult(newValues, mapping);
  }

  private Mapping createMapping(BitSet fieldsUsed, int fieldCount) {
    final Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION,
        fieldCount, fieldsUsed.cardinality());
    int i = 0;
    for (int field : BitSets.toIter(fieldsUsed)) {
      mapping.set(field, i++);
    }
    return mapping;
  }

  /**
   * Variant of {@link #trimFields(RelNode, BitSet, Set)} for
   * {@link org.eigenbase.rel.TableAccessRel}.
   */
  public TrimResult trimFields(final HiveTableScanRel tableAccessRel,
      BitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    final int fieldCount = tableAccessRel.getRowType().getFieldCount();
    if (fieldsUsed.equals(BitSets.range(fieldCount)) && extraFields.isEmpty()) {
      return trimFields((HiveRel) tableAccessRel, fieldsUsed, extraFields);
    }
    final RelNode _newTableAccessRel = tableAccessRel.project(fieldsUsed,
        extraFields);
    final RelNode newTableAccessRel = HiveProjectRel.DEFAULT_PROJECT_FACTORY
        .createProject(tableAccessRel, _newTableAccessRel.getChildExps(),
            _newTableAccessRel.getRowType().getFieldNames());
    final Mapping mapping = createMapping(fieldsUsed, fieldCount);
    return new TrimResult(newTableAccessRel, mapping);
  }

  // ~ Inner Classes ----------------------------------------------------------

  /**
   * Result of an attempt to trim columns from a relational expression.
   *
   * <p>
   * The mapping describes where to find the columns wanted by the parent of the
   * current relational expression.
   *
   * <p>
   * The mapping is a {@link org.eigenbase.util.mapping.Mappings.SourceMapping},
   * which means that no column can be used more than once, and some columns are
   * not used. {@code columnsUsed.getSource(i)} returns the source of the i'th
   * output field.
   *
   * <p>
   * For example, consider the mapping for a relational expression that has 4
   * output columns but only two are being used. The mapping {2 &rarr; 1, 3
   * &rarr; 0} would give the following behavior:
   * </p>
   *
   * <ul>
   * <li>columnsUsed.getSourceCount() returns 4
   * <li>columnsUsed.getTargetCount() returns 2
   * <li>columnsUsed.getSource(0) returns 3
   * <li>columnsUsed.getSource(1) returns 2
   * <li>columnsUsed.getSource(2) throws IndexOutOfBounds
   * <li>columnsUsed.getTargetOpt(3) returns 0
   * <li>columnsUsed.getTargetOpt(0) returns -1
   * </ul>
   */
  protected static class TrimResult extends Pair<RelNode, Mapping> {
    /**
     * Creates a TrimResult.
     *
     * @param left
     *          New relational expression
     * @param right
     *          Mapping of fields onto original fields
     */
    public TrimResult(RelNode left, Mapping right) {
      super(left, right);
    }
  }
}

// End RelFieldTrimmer.java

