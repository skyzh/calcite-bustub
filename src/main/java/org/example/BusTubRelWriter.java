package org.example;


import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Callback for a relational expression to dump itself as JSON.
 *
 * @see RelJsonReader
 */
public class BusTubRelWriter implements RelWriter {
    protected final JsonBuilder jsonBuilder;
    protected final BusTubRelJson relJson;
    private final IdentityHashMap<RelNode, String> relIdMap = new IdentityHashMap<>();
    protected final List<@Nullable Object> relList;
    private final List<Pair<String, @Nullable Object>> values = new ArrayList<>();
    private @Nullable String previousId;

    //~ Constructors -------------------------------------------------------------

    /**
     * Creates a RelJsonWriter with a private JsonBuilder.
     */
    public BusTubRelWriter() {
        this(new JsonBuilder());
    }

    /**
     * Creates a RelJsonWriter with a given JsonBuilder.
     */
    public BusTubRelWriter(JsonBuilder jsonBuilder) {
        this(jsonBuilder, UnaryOperator.identity());
    }

    /**
     * Creates a RelJsonWriter.
     */
    public BusTubRelWriter(JsonBuilder jsonBuilder,
                           UnaryOperator<RelJson> relJsonTransform) {
        this.jsonBuilder = requireNonNull(jsonBuilder, "jsonBuilder");
        relList = this.jsonBuilder.list();
        relJson = new BusTubRelJson(jsonBuilder);
    }

    //~ Methods ------------------------------------------------------------------

    private String getFieldType(RelDataTypeField field) {
        return field.getType().toString();
    }

    protected void explain_(RelNode rel, List<Pair<String, @Nullable Object>> values) {
        final Map<String, @Nullable Object> map = jsonBuilder.map();

        map.put("id", null); // ensure that id is the first attribute
        map.put("relOp", relJson.classToTypeName(rel.getClass()));
        for (Pair<String, @Nullable Object> value : values) {
            if (value.right instanceof RelNode) {
                continue;
            }
            put(map, value.left, value.right);
        }
        // omit 'inputs: ["3"]' if "3" is the preceding rel
        final List<@Nullable Object> list = explainInputs(rel.getInputs());
        map.put("inputs", list);

        final String id = Integer.toString(relIdMap.size());
        relIdMap.put(rel, id);
        map.put("id", id);
        map.put("fields", rel.getRowType().getFieldNames());
        map.put("fieldTypes", rel.getRowType().getFieldList().stream().map(this::getFieldType)
                .collect(Collectors.toList()));

        relList.add(map);
        previousId = id;
    }

    private void put(Map<String, @Nullable Object> map, String name, @Nullable Object value) {
        map.put(name, relJson.toJson(value));
    }

    private List<@Nullable Object> explainInputs(List<RelNode> inputs) {
        final List<@Nullable Object> list = jsonBuilder.list();
        for (RelNode input : inputs) {
            String id = relIdMap.get(input);
            if (id == null) {
                input.explain(this);
                id = previousId;
            }
            list.add(id);
        }
        return list;
    }

    @Override
    public final void explain(RelNode rel, List<Pair<String, @Nullable Object>> valueList) {
        explain_(rel, valueList);
    }

    @Override
    public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.ALL_ATTRIBUTES;
    }

    @Override
    public RelWriter item(String term, @Nullable Object value) {
        values.add(Pair.of(term, value));
        return this;
    }

    @Override
    public RelWriter done(RelNode node) {
        final List<Pair<String, @Nullable Object>> valuesCopy =
                ImmutableList.copyOf(values);
        values.clear();
        explain_(node, valuesCopy);
        return this;
    }

    @Override
    public boolean nest() {
        return true;
    }

    /**
     * Returns a JSON string describing the relational expressions that were just
     * explained.
     */
    public String asString() {
        final Map<String, @Nullable Object> map = jsonBuilder.map();
        map.put("rels", relList);
        return jsonBuilder.toJsonString(map);
    }
}
