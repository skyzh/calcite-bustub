package org.example;

import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BusTubSort extends Sort implements BindableRel {
    public BusTubSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RelCollation collation) {
        super(cluster, traits, child, collation, null, null);
    }


    @Override
    public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, @Nullable RexNode offset, @Nullable RexNode fetch) {
        return new BusTubSort(getCluster(), traitSet, newInput, newCollation);
    }

    @Override
    public Node implement(InterpreterImplementor implementor) {
        return null;
    }

    @Override
    public Class<Object[]> getElementType() {
        return null;
    }

    @Override
    public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
        return null;
    }
}
