/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.argus.columnar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dva.argus.entity.Annotation;
import com.salesforce.dva.argus.entity.Histogram;
import com.salesforce.dva.argus.entity.Metric;
import com.salesforce.dva.argus.service.DefaultService;
import com.salesforce.dva.argus.service.TSDBService;
import com.salesforce.dva.argus.service.tsdb.AnnotationQuery;
import com.salesforce.dva.argus.service.tsdb.MetricQuery;
import com.salesforce.dva.argus.service.tsdb.MetricQuery.Aggregator;
import com.salesforce.dva.argus.system.SystemAssert;
import com.salesforce.dva.argus.system.SystemConfiguration;
import com.salesforce.dva.argus.system.SystemException;
import io.airlift.log.Logger;
import io.prestosql.jdbc.PrestoConnection;
import io.prestosql.jdbc.PrestoDriver;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.salesforce.dva.argus.service.tsdb.MetricQuery.Aggregator.NONE;
import static io.prestosql.plugin.argus.columnar.PrestoTSDBService.TSDBColumn.METRIC;
import static io.prestosql.plugin.argus.columnar.PrestoTSDBService.TSDBColumn.SCOPE;
import static io.prestosql.plugin.argus.columnar.PrestoTSDBService.TSDBColumn.TAGS;
import static io.prestosql.plugin.argus.columnar.PrestoTSDBService.TSDBColumn.TIME;
import static io.prestosql.plugin.argus.columnar.PrestoTSDBService.TSDBColumn.VALUE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class PrestoTSDBService
        extends DefaultService
        implements TSDBService
{
    protected final Logger logger = Logger.get(PrestoTSDBService.class);
    private final String prestoJDBCUrl;
    private final String metricsTableName;
    private final PrestoDriver prestoDriver = new PrestoDriver();
    private final Properties connectionProperties;

    @Inject
    public PrestoTSDBService(ArgusColumnarConfig config)
    {
        this(new SystemConfiguration(toProperties(config)));
    }

    private static Properties toProperties(ArgusColumnarConfig config)
    {
        Properties props = new Properties();
        props.setProperty(Property.PRESTO_JDBC_URL.getName(), config.getPrestoJdbcUrl());
        props.setProperty(Property.PRESTO_CATALOG_NAME.getName(), config.getCatalogName());
        props.setProperty(Property.PRESTO_SCHEMA_NAME.getName(), config.getSchemaName());
        props.setProperty(Property.PRESTO_TABLE_NAME.getName(), config.getTableName());
        props.setProperty(Property.PRESTO_USER_NAME.getName(), config.getUserName());
        return props;
    }

    protected PrestoTSDBService(SystemConfiguration config)
    {
        super(config);
        requireNonNull(config, "System configuration cannot be null.");
        prestoJDBCUrl = config.getValue(Property.PRESTO_JDBC_URL.getName(), Property.PRESTO_JDBC_URL.getDefaultValue());
        metricsTableName = getFullTableName(config);
        Properties properties = new Properties();
        properties.setProperty("user", config.getValue(Property.PRESTO_USER_NAME.getName(), Property.PRESTO_USER_NAME.getDefaultValue()));
        this.connectionProperties = properties;
    }

    private String getFullTableName(SystemConfiguration config)
    {
        String catalog = config.getValue(Property.PRESTO_CATALOG_NAME.getName(), Property.PRESTO_CATALOG_NAME.getDefaultValue());
        String schema = config.getValue(Property.PRESTO_SCHEMA_NAME.getName(), Property.PRESTO_SCHEMA_NAME.getDefaultValue());
        String table = config.getValue(Property.PRESTO_TABLE_NAME.getName(), Property.PRESTO_TABLE_NAME.getDefaultValue());
        return String.format("%s.%s.%s", catalog, schema, table);
    }

    @Override
    public Map<MetricQuery, List<Metric>> getMetrics(List<MetricQuery> queries)
    {
        SystemAssert.requireArgument(queries != null, "Metric queries list cannot be null.");

        Map<MetricQuery, List<Metric>> result = new HashMap<>();
        for (MetricQuery query : queries) {
            result.put(query, selectMetrics(query));
        }
        return result;
    }

    List<Metric> selectMetrics(MetricQuery metricQuery)
    {
        Map<String, Metric> metrics = new HashMap<>();
        List<TypeAndValue> bindings = new ArrayList<>();
        String selectQuery = getPrestoQuery(metricQuery, bindings);
        logger.debug("Executing query: " + selectQuery);
        try (Connection connection = prestoDriver.connect(prestoJDBCUrl, connectionProperties)) {
            connection.unwrap(PrestoConnection.class).setTimeZoneId("UTC");
            PreparedStatement preparedStmt = prepareStatement(metricQuery, selectQuery, connection, bindings);
            ResultSet rs = preparedStmt.executeQuery();
            readResults(metricQuery, metrics, rs);
        }
        catch (SQLException sqle) {
            throw new SystemException("Failed to read data from Presto. Query: " + selectQuery, sqle);
        }

        return new ArrayList<>(metrics.values());
    }

    private PreparedStatement prepareStatement(MetricQuery metricQuery, String selectQuery, Connection connection, List<TypeAndValue> bindings)
            throws SQLException
    {
        PreparedStatement preparedStmt = connection.prepareStatement(selectQuery);
        int index = 1;

        for (TypeAndValue typeAndValue : bindings) {
            Type type = typeAndValue.getType();
            Object value = typeAndValue.getValue();
            if (DOUBLE.equals(type)) {
                preparedStmt.setDouble(index++, (double) value);
            }
            else if (VARCHAR.equals(type)) {
                preparedStmt.setString(index++, (String) value);
            }
            else if (TIMESTAMP.equals(type)) {
                preparedStmt.setTimestamp(index++, (Timestamp) value);
            }
            else {
                throw new SystemException("Unsupported binding type: " + type);
            }
        }
        return preparedStmt;
    }

    @SuppressWarnings("unchecked")
    private void readResults(MetricQuery metricQuery, Map<String, Metric> metrics, ResultSet rs)
            throws SQLException
    {
        while (rs.next()) {
            String scope = rs.getString("scope");
            String metricName = rs.getString("metric");
            Double value = rs.getDouble("value");
            long timestamp = rs.getTimestamp("time").getTime();

            Map<String, String> tags;

            if (hasNoAggregator(metricQuery)) {
                tags = (Map<String, String>) rs.getObject("tags");
            }
            else {
                tags = new HashMap<>();
                for (String tagKey : metricQuery.getTags().keySet()) {
                    tags.put(tagKey, rs.getString(tagKey));
                }
            }

            Map<Long, Double> datapoints = new HashMap<>();
            datapoints.put(timestamp, value);
            String identifier = scope + metricName + tags.toString();
            if (metrics.containsKey(identifier)) {
                metrics.get(identifier).addDatapoints(datapoints);
            }
            else {
                Metric metric = new Metric(scope, metricName);
                metric.setTags(tags);
                metric.setDatapoints(datapoints);
                metrics.put(identifier, metric);
            }
        }
    }

    private Timestamp getUtcTimestamp(Long endTimestamp)
    {
        Timestamp endTs = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(endTimestamp), ZoneId.of("UTC")));
        return endTs;
    }

    protected String getPrestoQuery(MetricQuery query, List<TypeAndValue> bindings)
    {
        Map<String, String> tags = query.getTags();
        // TODO no way to provide aliased col name as a param, since they must be provided before param binding,
        // so need to find another way to do this
        String aliasedMapCols = tags.keySet().stream()
                .map(tagKey -> MessageFormat.format("tags[''{0}''] as \"{0}\"", tagKey))
                .collect(joining(","));

        String groupByOrdinals = IntStream.range(1, tags.size() + 1)
                .mapToObj(Integer::toString)
                .collect(joining(",", "", tags.size() > 0 ? "," : ""));
        String groupByClause = MessageFormat.format("GROUP BY {0} {1}, {2}, {3}", groupByOrdinals, TIME, SCOPE, METRIC);
        if (hasNoAggregator(query)) {
            groupByClause = "";
            aliasedMapCols = TAGS.toString();
            groupByOrdinals = "1, ";
        }

        String agg = convertArgusAggregatorToPrestoAggregator(query.getAggregator());
        Map<TSDBColumn, String> expressions = ImmutableMap.of(
                TAGS, aliasedMapCols,
                VALUE, aggregatedValueProjection(agg));
        String projection = buildProjection(query, expressions);

        StringBuilder filtersBuilder = buildFilters(query, bindings);

        String selectSql;
        if (query.getDownsampler() != null) {
            String downsamplingSql = buildDownsamplingQuery(query, metricsTableName, filtersBuilder, aliasedMapCols, groupByOrdinals, bindings);
            if (query.getDownsampler().equals(query.getAggregator())) {
                return downsamplingSql;
            }
            String tagKeyCols = tags.keySet().stream().map(tagKey -> "\"" + tagKey + "\"").collect(joining(","));
            if (hasNoAggregator(query)) {
                tagKeyCols = TAGS.toString();
            }

            expressions = ImmutableMap.of(
                    TAGS, tagKeyCols,
                    VALUE, aggregatedValueProjection(agg));
            projection = buildProjection(query, expressions);
            selectSql = MessageFormat.format(" WITH downsampled AS ({0}) SELECT {1} FROM downsampled {2}",
                    downsamplingSql, projection, groupByClause);
        }
        else {
            filtersBuilder.append(MessageFormat.format(" AND {0} >= ? AND {0} <= ?", TIME));
            bindings.add(new TypeAndValue(TIMESTAMP, getUtcTimestamp(query.getStartTimestamp())));
            bindings.add(new TypeAndValue(TIMESTAMP, getUtcTimestamp(query.getEndTimestamp())));
            selectSql = MessageFormat.format(
                    "SELECT {0} FROM {1} WHERE {2} {3}",
                    projection, metricsTableName, filtersBuilder, groupByClause);
        }
        return selectSql;
    }

    protected StringBuilder buildFilters(MetricQuery query, List<TypeAndValue> bindings)
    {
        StringBuilder filters = new StringBuilder();

        if (!isNullOrEmpty(query.getScope()) && !query.getScope().equals("*")) {
            filters.append(MessageFormat.format("{0} IN {1}", SCOPE, toParamString(query.getScope(), bindings)));
        }

        if (!isNullOrEmpty(query.getMetric()) && !query.getMetric().equals("*")) {
            filters.append(MessageFormat.format(" AND {0} IN {1}", METRIC, toParamString(query.getMetric(), bindings)));
        }

        String tagsFilter = query.getTags().entrySet().stream()
                .filter(entry -> !entry.getValue().equals("*"))
                .peek(entry -> {
                    bindings.add(new TypeAndValue(VARCHAR, entry.getKey()));
                    for (String orValue : entry.getValue().split("\\|")) {
                        bindings.add(new TypeAndValue(VARCHAR, orValue));
                    }
                })
                .map(entry -> MessageFormat.format(
                        " AND element_at({0}, ?) IN ({1})",
                        TAGS,
                        // convert "tagA|tagB|tagC" to "?,?,?"
                        Arrays.stream(entry.getValue().split("\\|")).map(value -> "?").collect(joining(","))))
                .collect(joining(","));
        filters.append(tagsFilter);

        return filters;
    }

    private static String aggregatedValueProjection(String agg)
    {
        return MessageFormat.format("{0}({1}) AS {1}", agg, VALUE);
    }

    private static String toParamString(String delimited, List<TypeAndValue> bindings)
    {
        return Arrays.stream(delimited.split("\\|"))
                .peek(value -> bindings.add(new TypeAndValue(VARCHAR, value)))
                .map(scope -> "?")
                .collect(Collectors.joining(",", "(", ")"));
    }

    private String buildDownsamplingQuery(MetricQuery query, String metricsTableName, StringBuilder filtersBuilder, String aliasedMapCols, String groupByOrdinals, List<TypeAndValue> bindings)
    {
        String downsamplingAgg = convertArgusAggregatorToPrestoAggregator(query.getDownsampler());
        long downsamplingPeriodSec = TimeUnit.SECONDS.convert(query.getDownsamplingPeriod(), TimeUnit.MILLISECONDS);
        String timeDownsampleFunction = MessageFormat.format("from_unixtime(to_unixtime(time) - (to_unixtime(time) % {0}))", Long.toString(downsamplingPeriodSec));
        String startTimeParameter = MessageFormat.format("from_unixtime(to_unixtime(?) - (to_unixtime(?) % {0}))", Long.toString(downsamplingPeriodSec));
        // to make the results the same as an OTSDB query,
        // need to fetch all the data points required for aggregating the last time interval
        // e.g. if end_time=timestamp '2019-07-01 15:00' and downsampler='15m-zimsum',
        // we need to query up to 15:15 to be able to downsample to 15:00
        String endTimeParameter = MessageFormat.format("from_unixtime((to_unixtime(?) + {0}) - (to_unixtime(?) % {0}))", Long.toString(downsamplingPeriodSec));
        filtersBuilder.append(MessageFormat.format(" AND {0} >= {1} AND {0} <= {2}", TIME, startTimeParameter, endTimeParameter));
        bindings.add(new TypeAndValue(TIMESTAMP, getUtcTimestamp(query.getStartTimestamp())));
        bindings.add(new TypeAndValue(TIMESTAMP, getUtcTimestamp(query.getStartTimestamp())));
        bindings.add(new TypeAndValue(TIMESTAMP, getUtcTimestamp(query.getEndTimestamp())));
        bindings.add(new TypeAndValue(TIMESTAMP, getUtcTimestamp(query.getEndTimestamp())));

        // If no tags are specified, we need to downsample before aggregating.
        // Specifying 'tags' groupBy here makes it so we don't aggregate.
        // But if downsampler=aggregator, we *do* want the aggregating to happen, as an optimization to do everything in one query
        if (!query.getDownsampler().equals(query.getAggregator()) && isNullOrEmpty(aliasedMapCols)) {
            aliasedMapCols = TAGS.toString();
            groupByOrdinals = "1,";
        }
        String groupByColumns = MessageFormat.format("{0} {1}, {2}, {3}", groupByOrdinals, SCOPE, METRIC, timeDownsampleFunction);

        Map<TSDBColumn, String> expressions = ImmutableMap.of(
                TAGS, aliasedMapCols,
                VALUE, aggregatedValueProjection(downsamplingAgg),
                TIME, timeDownsampleFunction + " AS " + TIME.toString());
        String projection = buildProjection(query, expressions);

        String downsamplingSql = MessageFormat.format(
                "SELECT {0} "
                        + "FROM {1} "
                        + "WHERE {2} "
                        + "GROUP BY {3} ",
                projection,
                metricsTableName,
                filtersBuilder,
                groupByColumns);
        return downsamplingSql;
    }

    private String buildProjection(MetricQuery query, Map<TSDBColumn, String> expressions)
    {
        return getProjectedColumns(query).stream()
                .map(columnName -> expressions.containsKey(columnName) ? expressions.get(columnName) : columnName.name().toLowerCase(ENGLISH))
                .filter(name -> name.length() > 0)
                .collect(joining(","));
    }

    // Argus doesn't support projection, so return all columns by default
    protected List<TSDBColumn> getProjectedColumns(MetricQuery query)
    {
        return ALL_COLUMNS;
    }

    private static boolean hasNoAggregator(MetricQuery query)
    {
        return query.getAggregator() == null || query.getAggregator().equals(NONE);
    }

    private static String convertArgusAggregatorToPrestoAggregator(Aggregator aggregator)
    {
        if (aggregator == null) {
            return "";
        }

        // TODO currently no equivalent in Presto for aggregators with interpolation
        switch (aggregator) {
            case AVG:
                return "AVG";
            case ZIMSUM:
            case SUM:
                return "SUM";
            case MIMMIN:
            case MIN:
                return "MIN";
            case MIMMAX:
            case MAX:
                return "MAX";
            case DEV:
                return "STDDEV_POP";
            case COUNT:
                return "COUNT";
            case NONE:
                // if no aggreagtor, return all tags
                return "";
            default:
                throw new UnsupportedOperationException("Unsupported aggregator: " + aggregator);
        }
    }

    @Override
    public void putAnnotations(List<Annotation> annotations)
    {
        throw new UnsupportedOperationException("putAnnotations not implemented");
    }

    @Override
    public List<Annotation> getAnnotations(List<AnnotationQuery> queries)
    {
        throw new UnsupportedOperationException("getAnnotations not implemented");
    }

    @Override
    public void putMetrics(List<Metric> metrics)
    {
        throw new UnsupportedOperationException("putMetrics not implemented");
    }

    @Override
    public void putHistograms(List<Histogram> histograms)
    {
        throw new UnsupportedOperationException("putHistograms not implemented");
    }

    // order currently matters for TAGS, since it's referenced in groupByOrdinals
    public static final List<TSDBColumn> ALL_COLUMNS = ImmutableList.of(TAGS, SCOPE, METRIC, TIME, VALUE);

    public static class TypeAndValue
    {
        private final Type type;
        private final Object value;

        public TypeAndValue(Type type, Object value)
        {
            this.type = requireNonNull(type, "type is null");
            this.value = requireNonNull(value, "value is null");
        }

        public Type getType()
        {
            return type;
        }

        public Object getValue()
        {
            return value;
        }
    }

    public static enum TSDBColumn
    {
        TAGS, SCOPE, METRIC, TIME, VALUE;

        @Override
        public String toString()
        {
            return super.toString().toLowerCase(ENGLISH);
        }
    }

    public enum Property
    {
        /**
         * The TSDB read endpoint.
         */
        PRESTO_JDBC_URL("service.property.tsdb.presto.jdbc.url", "jdbc:presto://localhost:8080"),
        PRESTO_CATALOG_NAME("service.property.tsdb.presto.catalog.name", "hive"),
        PRESTO_SCHEMA_NAME("service.property.tsdb.presto.schema.name", "s3"),
        PRESTO_TABLE_NAME("service.property.tsdb.presto.table.name", "orc_flat_metrics"),
        PRESTO_USER_NAME("service.property.tsdb.presto.user.name", "someUser");

        private final String name;
        private final String defaultValue;

        private Property(String name, String defaultValue)
        {
            this.name = name;
            this.defaultValue = defaultValue;
        }

        /**
         * Returns the property name.
         *
         * @return The property name.
         */
        public String getName()
        {
            return name;
        }

        /**
         * Returns the default value for the property.
         *
         * @return The default value.
         */
        public String getDefaultValue()
        {
            return defaultValue;
        }
    }
}
