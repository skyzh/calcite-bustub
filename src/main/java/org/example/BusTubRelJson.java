package org.example;

import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.JsonBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONObject;

import java.util.Map;

public class BusTubRelJson extends RelJson {
    public BusTubRelJson(@Nullable JsonBuilder jsonBuilder) {
        super(jsonBuilder);
    }

    public Object toJson(RexNode node) {
        Map<String, @Nullable Object> map = (Map<String, @Nullable Object>) super.toJson(node);
        map.put("outType", node.getType().toString());
        System.out.println(node.toString());
        return map;
    }
}
