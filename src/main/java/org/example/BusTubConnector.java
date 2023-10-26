package org.example;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;


public class BusTubConnector {
    private String addr;

    BusTubConnector(String addr) {
        this.addr = addr;
    }

    String runQuery(String query) throws IOException {
        final HttpPost httpPost = new HttpPost(addr + "/sql");
        CloseableHttpClient client = HttpClients.createDefault();
        JSONObject obj = new JSONObject();
        obj.put("sql", query);
        StringEntity requestEntity = new StringEntity(
                obj.toString(),
                ContentType.APPLICATION_JSON);
        httpPost.setEntity(requestEntity);
        CloseableHttpResponse response = (CloseableHttpResponse) client
                .execute(httpPost);
        BufferedReader rd = new BufferedReader
                (new InputStreamReader(
                        response.getEntity().getContent()));
        String line = rd.lines().collect(Collectors.joining(System.lineSeparator()));
        return line;
    }

    String serializePlan(BindableRel plan) {
        RelWriter planWriter = new BusTubRelWriter();
        plan.explain(planWriter);
        return ((BusTubRelWriter) planWriter).asString();
    }


    String runPlan(BindableRel plan) throws IOException {
        final HttpPost httpPost = new HttpPost(addr + "/plan");
        CloseableHttpClient client = HttpClients.createDefault();
        String obj = serializePlan(plan);
        StringEntity requestEntity = new StringEntity(
                obj,
                ContentType.APPLICATION_JSON);
        httpPost.setEntity(requestEntity);
        CloseableHttpResponse response = (CloseableHttpResponse) client
                .execute(httpPost);
        BufferedReader rd = new BufferedReader
                (new InputStreamReader(
                        response.getEntity().getContent()));
        String line = rd.lines().collect(Collectors.joining(System.lineSeparator()));
        return line;
    }

    CalciteSchema retrieveCatalog(RelDataTypeFactory typeFactory) throws IOException {
        final HttpGet httpGet = new HttpGet(addr + "/catalog");

        CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = (CloseableHttpResponse) client
                .execute(httpGet);
        BufferedReader rd = new BufferedReader
                (new InputStreamReader(
                        response.getEntity().getContent()));
        String line = rd.lines().collect(Collectors.joining(System.lineSeparator()));
        JSONObject obj = new JSONObject(line);
        JSONArray tables = obj.getJSONArray("catalog");
        CalciteSchema schema = CalciteSchema.createRootSchema(true);
        for (Object table_obj : tables) {
            RelDataTypeFactory.Builder typ = new RelDataTypeFactory.Builder(typeFactory);
            JSONObject table = (JSONObject) table_obj;
            JSONArray fields = table.getJSONArray("schema");
            for (Object col_obj : fields) {
                JSONObject col = (JSONObject) col_obj;
                SqlTypeName typ_name;
                String type_name = col.getString("type");
                if (type_name.equals("integer")) {
                    typ_name = SqlTypeName.INTEGER;
                } else if (type_name.equals("varchar")) {
                    typ_name = SqlTypeName.VARCHAR;
                } else {
                    typ_name = SqlTypeName.ANY;
                }
                typ.add(col.getString("name"), typ_name);
            }
            ListTable table_list = new ListTable(typ.build(), Arrays.asList());
            schema.add(table.getString("name"), table_list);
        }
        return schema;
    }

    public static class BusTubRelDataTypeFactory extends JavaTypeFactoryImpl {
        @Override
        public Charset getDefaultCharset() {
            return Charset.forName("UTF8");
        }
    }

    public void run() throws Exception {
        // Instantiate a type factory for creating types (e.g., VARCHAR, NUMERIC, etc.)
        RelDataTypeFactory typeFactory = new BusTubRelDataTypeFactory();

        Scanner userInput = new Scanner(System.in);

        // SELECT max(book.title) FROM book INNER JOIN author ON book.author = author.id GROUP BY book.author

        while (true) {
            try {
                String input = userInput.nextLine();

                CalciteSchema schema = retrieveCatalog(typeFactory);

                SqlParser parser = SqlParser.create(input);

                // Parse the query into an AST
                SqlNode sqlNode;

                try {
                    sqlNode = parser.parseQuery();
                } catch (SqlParseException e) {
                    System.err.println("cannot parse with Calcite, forwarding to BusTub");
                    System.out.println(runQuery(input));
                    continue;
                }

                // Configure and instantiate validator`
                Properties props = new Properties();
                props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
                CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
                CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
                        Collections.singletonList(""),
                        typeFactory, config);

                SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                        catalogReader, typeFactory,
                        SqlValidator.Config.DEFAULT);

                // Validate the initial AST
                SqlNode validNode = validator.validate(sqlNode);


                // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
                RelOptCluster cluster = newCluster(typeFactory);
                SqlToRelConverter relConverter = new SqlToRelConverter(
                        NOOP_EXPANDER,
                        validator,
                        catalogReader,
                        cluster,
                        StandardConvertletTable.INSTANCE,
                        SqlToRelConverter.config());

                // Convert the valid AST into a logical plan
                RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

                // Display the logical plan
                System.out.println(
                        RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                                SqlExplainLevel.EXPPLAN_ATTRIBUTES));

                // Initialize optimizer/planner with the necessary rules
                RelOptPlanner planner = cluster.getPlanner();
                planner.addRule(CoreRules.FILTER_INTO_JOIN);
                planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
                planner.addRule(Bindables.BINDABLE_FILTER_RULE);
                planner.addRule(Bindables.BINDABLE_JOIN_RULE);
                planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
//                planner.addRule(Bindables.BINDABLE_SORT_RULE);
                planner.addRule(Bindables.BINDABLE_AGGREGATE_RULE);
                planner.addRule(SortConvertRule.INSTANCE);

                // Define the type of the output plan (in this case we want a physical plan in
                // BindableConvention)
                logPlan = planner.changeTraits(logPlan,
                        cluster.traitSet().replace(BindableConvention.INSTANCE));
                planner.setRoot(logPlan);
                // Start the optimization process to obtain the most efficient physical plan based on the
                // provided rule set.
                BindableRel phyPlan;

                try {
                    phyPlan =
                            (BindableRel) planner.findBestExp();
                } catch (RelOptPlanner.CannotPlanException e) {
                    System.err.println(e);
                    continue;
                }

                // Display the physical plan
                System.out.println(
                        RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                                SqlExplainLevel.NON_COST_ATTRIBUTES));

                System.out.println(runPlan(phyPlan));
            } catch (CalciteException e) {
                System.out.println(e);
            }
        }
    }

    /**
     * A simple table based on a list.
     */
    private static class ListTable extends AbstractTable implements ScannableTable {
        private final RelDataType rowType;
        private final List<Object[]> data;

        ListTable(RelDataType rowType, List<Object[]> data) {
            this.rowType = rowType;
            this.data = data;
        }

        @Override
        public Enumerable<Object[]> scan(final DataContext root) {
            return Linq4j.asEnumerable(data);
        }

        @Override
        public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
            return rowType;
        }
    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
            , viewPath) -> null;

    /**
     * A simple data context only with schema information.
     */
    private static final class SchemaOnlyDataContext implements DataContext {
        private final SchemaPlus schema;

        SchemaOnlyDataContext(CalciteSchema calciteSchema) {
            this.schema = calciteSchema.plus();
        }

        @Override
        public SchemaPlus getRootSchema() {
            return schema;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return new JavaTypeFactoryImpl();
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(final String name) {
            return null;
        }
    }

}
