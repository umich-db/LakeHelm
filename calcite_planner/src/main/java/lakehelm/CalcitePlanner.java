package lakehelm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Optimizes SQL with Apache Calcite and prints the logical plans as JSON.
 *
 * <p>Input (file argument or stdin):
 * <pre>
 * {"tables": {"orders": {"rows": 1500000,
 *                        "columns": [{"name": "o_orderkey", "type": "BIGINT"}, ...]}, ...},
 *  "queries": [{"name": "q3", "sql": "SELECT ..."}, ...]}
 * </pre>
 * Output: {"plans": [{"name": ..., "plan": {node}} | {"name": ..., "error": ...}]}, where a
 * node is {"op", "kind", "rows", "children", ...} and "rows" is Calcite's row-count estimate
 * (driven by the table row counts given in the input).
 */
public final class CalcitePlanner {
  private static final ObjectMapper JSON = new ObjectMapper();

  private CalcitePlanner() {}

  /** A table with a fixed row type and a row-count statistic (no data). */
  static final class StatTable extends AbstractTable {
    private final JsonNode columns;
    private final double rows;

    StatTable(JsonNode columns, double rows) {
      this.columns = columns;
      this.rows = rows;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory tf) {
      RelDataTypeFactory.Builder b = tf.builder();
      for (JsonNode c : columns) {
        b.add(c.get("name").asText(), sqlType(tf, c.path("type").asText("VARCHAR")));
      }
      return b.build();
    }

    @Override public Statistic getStatistic() {
      return Statistics.of(rows, ImmutableList.of());
    }
  }

  static RelDataType sqlType(RelDataTypeFactory tf, String t) {
    String u = t.trim().toUpperCase(Locale.ROOT);
    String base = u.replaceAll("\\(.*", "").trim();
    SqlTypeName name;
    switch (base) {
      case "INT": case "INTEGER": name = SqlTypeName.INTEGER; break;
      case "BIGINT": case "LONG": name = SqlTypeName.BIGINT; break;
      case "SMALLINT": name = SqlTypeName.SMALLINT; break;
      case "TINYINT": name = SqlTypeName.TINYINT; break;
      case "DOUBLE": case "FLOAT": case "REAL": name = SqlTypeName.DOUBLE; break;
      case "DECIMAL": case "NUMERIC": name = SqlTypeName.DECIMAL; break;
      case "DATE": name = SqlTypeName.DATE; break;
      case "TIMESTAMP": name = SqlTypeName.TIMESTAMP; break;
      case "BOOLEAN": case "BOOL": name = SqlTypeName.BOOLEAN; break;
      case "CHAR": name = SqlTypeName.CHAR; break;
      default: name = SqlTypeName.VARCHAR;
    }
    if (name == SqlTypeName.DECIMAL) {
      java.util.regex.Matcher m = java.util.regex.Pattern
          .compile("\\((\\d+)\\s*,\\s*(\\d+)\\)").matcher(u);
      return m.find()
          ? tf.createTypeWithNullability(tf.createSqlType(name,
              Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2))), true)
          : tf.createTypeWithNullability(tf.createSqlType(name, 18, 4), true);
    }
    if (name == SqlTypeName.CHAR || name == SqlTypeName.VARCHAR) {
      java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\((\\d+)\\)").matcher(u);
      int len = m.find() ? Integer.parseInt(m.group(1)) : 1024;
      return tf.createTypeWithNullability(tf.createSqlType(name, len), true);
    }
    return tf.createTypeWithNullability(tf.createSqlType(name), true);
  }

  static RelNode optimize(RelNode rel) {
    // 1. sub-queries -> correlates -> joins
    HepProgramBuilder p1 = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
        .addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
    HepPlanner h1 = new HepPlanner(p1.build());
    h1.setRoot(rel);
    rel = h1.findBestExp();
    RelBuilder rb = RelBuilder.proto(org.apache.calcite.plan.Contexts.empty())
        .create(rel.getCluster(), null);
    rel = RelDecorrelator.decorrelateQuery(rel, rb);

    // 2. predicate push-down, expression reduction, projection cleanup
    HepProgramBuilder p2 = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleCollection(ImmutableList.of(
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_AGGREGATE_TRANSPOSE,
            CoreRules.FILTER_MERGE,
            CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_REMOVE,
            CoreRules.AGGREGATE_PROJECT_MERGE));
    HepPlanner h2 = new HepPlanner(p2.build());
    h2.setRoot(rel);
    rel = h2.findBestExp();

    // 3. cost-based join ordering with the row-count statistics
    HepProgramBuilder p3 = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE);
    HepPlanner h3 = new HepPlanner(p3.build());
    h3.setRoot(rel);
    rel = h3.findBestExp();

    // 4. final projection cleanup
    HepProgramBuilder p4 = new HepProgramBuilder()
        .addRuleCollection(ImmutableList.of(CoreRules.PROJECT_MERGE, CoreRules.PROJECT_REMOVE,
            CoreRules.FILTER_PROJECT_TRANSPOSE));
    HepPlanner h4 = new HepPlanner(p4.build());
    h4.setRoot(rel);
    return h4.findBestExp();
  }

  static String kind(RelNode n) {
    String t = n.getRelTypeName().replaceFirst("^(Logical|Enumerable|Jdbc)", "");
    if (n instanceof TableScan) return "Scan";
    if (n instanceof Join) {
      String jt = ((Join) n).getJoinType().name();
      return "Join " + jt.charAt(0) + jt.substring(1).toLowerCase(Locale.ROOT);
    }
    if (n instanceof Sort) {
      Sort s = (Sort) n;
      return s.getCollation().getFieldCollations().isEmpty() && s.fetch != null ? "Limit" : "Sort";
    }
    return t;
  }

  static ObjectNode toJson(RelNode n, RelMetadataQuery mq) {
    ObjectNode o = JSON.createObjectNode();
    o.put("op", n.getRelTypeName());
    o.put("kind", kind(n));
    Double rows = mq.getRowCount(n);
    o.put("rows", rows == null ? -1 : rows);
    o.put("fields", n.getRowType().getFieldCount());
    if (n instanceof TableScan) {
      List<String> qn = n.getTable().getQualifiedName();
      o.put("table", qn.get(qn.size() - 1));
      o.put("table_rows", n.getTable().getRowCount());
    }
    if (n instanceof Filter) {
      o.put("condition", ((Filter) n).getCondition().toString());
    }
    if (n instanceof Join) {
      o.put("condition", ((Join) n).getCondition().toString());
      o.put("join_type", ((Join) n).getJoinType().name());
    }
    if (n instanceof Aggregate) {
      o.put("group_keys", ((Aggregate) n).getGroupCount());
      o.put("agg_calls", ((Aggregate) n).getAggCallList().size());
    }
    if (n instanceof Sort) {
      Sort s = (Sort) n;
      o.put("sort_keys", s.getCollation().getFieldCollations().size());
      if (s.fetch instanceof RexLiteral) {
        o.put("fetch", ((RexLiteral) s.fetch).getValueAs(Long.class));
      }
    }
    ArrayNode ch = o.putArray("children");
    for (RelNode in : n.getInputs()) {
      ch.add(toJson(in, mq));
    }
    return o;
  }

  public static void main(String[] args) throws Exception {
    JsonNode in = args.length > 0 ? JSON.readTree(new File(args[0])) : JSON.readTree(System.in);
    SchemaPlus root = Frameworks.createRootSchema(true);
    JsonNode tables = in.get("tables");
    tables.fieldNames().forEachRemaining(t -> {
      JsonNode spec = tables.get(t);
      root.add(t, new StatTable(spec.get("columns"), spec.path("rows").asDouble(1000.0)));
    });
    FrameworkConfig cfg = Frameworks.newConfigBuilder()
        .defaultSchema(root)
        .parserConfig(SqlParser.config().withLex(Lex.MYSQL_ANSI).withCaseSensitive(false)
            .withConformance(SqlConformanceEnum.LENIENT))
        .sqlToRelConverterConfig(SqlToRelConverter.config().withExpand(false)
            .withTrimUnusedFields(true))
        .build();

    ObjectNode out = JSON.createObjectNode();
    ArrayNode plans = out.putArray("plans");
    for (JsonNode q : in.get("queries")) {
      ObjectNode r = plans.addObject();
      r.put("name", q.get("name").asText());
      Planner planner = Frameworks.getPlanner(cfg);
      try {
        String sql = q.get("sql").asText().trim().replaceAll(";\\s*$", "");
        SqlNode parsed = planner.parse(sql);
        SqlNode validated = planner.validate(parsed);
        RelNode rel = planner.rel(validated).project();
        RelNode best = optimize(rel);
        r.set("plan", toJson(best, best.getCluster().getMetadataQuery()));
        r.put("text", RelOptUtil.toString(best));
      } catch (Throwable e) {
        r.put("error", e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()));
      } finally {
        planner.close();
      }
    }
    JSON.writerWithDefaultPrettyPrinter().writeValue(System.out, out);
  }
}
