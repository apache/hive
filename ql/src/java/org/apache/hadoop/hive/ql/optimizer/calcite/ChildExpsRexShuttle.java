package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rex.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ChildExpsRexShuttle extends RexShuttle {
    private final List<RexNode> exps;

    public ChildExpsRexShuttle(List<RexNode> exps) {
        this.exps = exps;
    }

    @Override
    public RexNode visitOver(RexOver over) {
        exps.add(over);
        return over;
    }

    @Override
    public RexWindow visitWindow(RexWindow window) {
        exps.addAll(window.partitionKeys);
        return window;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
        exps.add(subQuery);
        return subQuery;
    }

    @Override
    public RexNode visitTableInputRef(RexTableInputRef ref) {
        exps.add(ref);
        return ref;
    }

    @Override
    public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        exps.add(fieldRef);
        return fieldRef;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        exps.add(call);
        return call;
    }

    @Override
    protected RexNode[] visitArray(RexNode[] exprs, boolean[] update) {
        exps.addAll(Arrays.asList(exprs));
        return exprs;
    }

    @Override
    protected List<RexNode> visitList(List<? extends RexNode> exprs, boolean[] update) {
        exps.addAll(exprs);
        return new ArrayList<>(exprs);
    }

    @Override
    protected List<RexFieldCollation> visitFieldCollations(List<RexFieldCollation> collations, boolean[] update) {
        for (RexFieldCollation rfc: collations) {
            exps.add(rfc.getKey());
        }
        return collations;
    }

    @Override
    public RexNode visitCorrelVariable(RexCorrelVariable variable) {
        exps.add(variable);
        return variable;
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
        exps.add(fieldAccess);
        return fieldAccess;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        exps.add(inputRef);
        return inputRef;
    }

    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) {
        exps.add(localRef);
        return localRef;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
        exps.add(literal);
        return literal;
    }

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        exps.add(dynamicParam);
        return dynamicParam;
    }

    @Override
    public RexNode visitRangeRef(RexRangeRef rangeRef) {
        exps.add(rangeRef);
        return rangeRef;
    }
}
