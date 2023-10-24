package org.example;

import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rule for converting logical sort to stream sort
 */
public class SortConvertRule extends ConverterRule {
    public static final SortConvertRule INSTANCE =
            Config.INSTANCE
                    .withRuleFactory(SortConvertRule::new)
                    .withOperandSupplier(t -> t.operand(LogicalSort.class).anyInputs())
                    .withDescription("convert to topN")
                    .as(Config.class)
                    .toRule(SortConvertRule.class);

    protected SortConvertRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        var sort = (LogicalSort) rel;

        if (sort.fetch == null && sort.offset == null) {
            final RelTraitSet traitSet =
                    sort.getTraitSet().replace(BindableConvention.INSTANCE);

            return new BusTubSort(rel.getCluster(), traitSet,
                    convert(sort.getInput(), traitSet), sort.getCollation());
        }

        if (sort.offset == null && sort.getSortExps().isEmpty()) {
            final RelTraitSet traitSet =
                    sort.getTraitSet().replace(BindableConvention.INSTANCE);

            return new BusTubLimit(rel.getCluster(), traitSet,
                    convert(sort.getInput(), traitSet), sort.getCollation(), sort.fetch);
        }


        if (sort.offset == null) {
            final RelTraitSet traitSet =
                    sort.getTraitSet().replace(BindableConvention.INSTANCE);

            return new BusTubTopN(rel.getCluster(), traitSet,
                    convert(sort.getInput(), traitSet), sort.getCollation(), sort.fetch);
        }


        return null;
    }
}