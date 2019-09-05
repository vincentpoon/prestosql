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

import com.google.common.annotations.VisibleForTesting;
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
import io.airlift.log.Logger;
import io.prestosql.jdbc.PrestoConnection;
import io.prestosql.jdbc.PrestoDriver;
import io.prestosql.plugin.argus.ArgusErrorCode;
import io.prestosql.spi.PrestoException;

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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.salesforce.dva.argus.service.tsdb.MetricQuery.Aggregator.NONE;
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

    //    @Inject
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
        String selectQuery = getPrestoQuery(metricQuery, metricsTableName);
        logger.debug("Executing query: " + selectQuery);
        try (Connection connection = prestoDriver.connect(prestoJDBCUrl, connectionProperties)) {
            connection.unwrap(PrestoConnection.class).setTimeZoneId("UTC");
            PreparedStatement preparedStmt = prepareStatement(metricQuery, selectQuery, connection);
            ResultSet rs = preparedStmt.executeQuery();
            readResults(metricQuery, metrics, rs);
        }
        catch (SQLException sqle) {
            throw new PrestoException(ArgusErrorCode.ARGUS_QUERY_ERROR, "Failed to read data from Presto. Query: " + selectQuery, sqle);
        }

        return new ArrayList<>(metrics.values());
    }

    private PreparedStatement prepareStatement(MetricQuery metricQuery, String selectQuery, Connection connection)
            throws SQLException
    {
        PreparedStatement preparedStmt = connection.prepareStatement(selectQuery);
        int index = 1;

        preparedStmt.setTimestamp(index++, getUtcTimestamp(metricQuery.getStartTimestamp()));
        if (metricQuery.getDownsampler() != null) {
            // startTimeParameter in getPrestoQuery
            preparedStmt.setTimestamp(index++, getUtcTimestamp(metricQuery.getStartTimestamp()));
        }

        preparedStmt.setTimestamp(index++, getUtcTimestamp(metricQuery.getEndTimestamp()));
        if (metricQuery.getDownsampler() != null) {
            // endTimeParameter in getPrestoQuery
            preparedStmt.setTimestamp(index++, getUtcTimestamp(metricQuery.getEndTimestamp()));
        }

        if (!isNullOrEmpty(metricQuery.getScope()) && !metricQuery.getScope().equals("*")) {
            for (String scope : metricQuery.getScope().split("\\|")) {
                preparedStmt.setString(index++, scope);
            }
        }

        if (!isNullOrEmpty(metricQuery.getMetric()) && !metricQuery.getMetric().equals("*")) {
            for (String metric : metricQuery.getMetric().split("\\|")) {
                preparedStmt.setString(index++, metric);
            }
        }

        if (!metricQuery.getTags().isEmpty()) {
            for (Entry<String, String> entry : metricQuery.getTags().entrySet()) {
                preparedStmt.setString(index++, entry.getKey());
                for (String orValue : entry.getValue().split("\\|")) {
                    preparedStmt.setString(index++, orValue);
                }
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

    @VisibleForTesting
    protected static String getPrestoQuery(MetricQuery query, String metricsTableName)
    {
        String scopeFilter = "";
        if (!isNullOrEmpty(query.getScope()) && !query.getScope().equals("*")) {
            scopeFilter = "AND scope IN " + toParamString(query.getScope());
        }
        String metricFilter = "";
        if (!isNullOrEmpty(query.getMetric()) && !query.getMetric().equals("*")) {
            metricFilter = "AND metric IN " + toParamString(query.getMetric());
        }
        String agg = convertArgusAggregatorToPrestoAggregator(query.getAggregator());

        Map<String, String> tags = query.getTags();
        String tagWhereClause = tags.entrySet().stream()
                .filter(entry -> !entry.getValue().equals("*"))
                .map(entry -> MessageFormat.format(
                        " AND element_at(tags, ?) IN ({0})",
                        // convert "tagA|tagB|tagC" to "?,?,?"
                        Arrays.stream(entry.getValue().split("\\|")).map(value -> "?").collect(joining(","))))
                .collect(joining(","));
        Collector<CharSequence, ?, String> tagsJoiner = joining(",", "", tags.size() > 0 ? "," : "");
        // TODO no way to provide aliased col name as a param, since they must be provided before param binding,
        // so need find another way to do this
        String aliasedMapCols = tags.keySet().stream()
                .map(tagKey -> MessageFormat.format("tags[''{0}''] as \"{0}\"", tagKey))
                .collect(tagsJoiner);

        String groupByOrdinals = IntStream.range(1, tags.size() + 1)
                .mapToObj(Integer::toString)
                .collect(tagsJoiner);
        String groupByClause = MessageFormat.format(" GROUP BY {0} time, scope, metric", groupByOrdinals);
        if (hasNoAggregator(query)) {
            groupByClause = "";
            aliasedMapCols = " tags, ";
            groupByOrdinals = "1, ";
        }

        String selectSql = MessageFormat.format("SELECT {0} {1}(value) value, time, scope, metric FROM {2}"
                        + " WHERE time >= ? AND time <= ? {3} {4} {5} {6}",
                aliasedMapCols, agg, metricsTableName, scopeFilter, metricFilter, tagWhereClause, groupByClause);

        if (query.getDownsampler() != null) {
            String downsamplingSql = buildDownsamplingQuery(query, metricsTableName, scopeFilter, metricFilter, tagWhereClause, aliasedMapCols, groupByOrdinals);
            if (query.getDownsampler().equals(query.getAggregator())) {
                return downsamplingSql;
            }
            String tagKeyCols = tags.keySet().stream().map(tagKey -> "\"" + tagKey + "\"").collect(tagsJoiner);
            if (hasNoAggregator(query)) {
                tagKeyCols = "tags,";
            }
            selectSql = MessageFormat.format(" WITH downsampled AS ({2}) SELECT {1} {0}(value) AS value, time as \"time\", scope, metric FROM downsampled {3}",
                    agg, tagKeyCols, downsamplingSql, groupByClause);
        }

        return selectSql;
    }

    private static String toParamString(String delimited)
    {
        return Arrays.stream(delimited.split("\\|"))
                .map(scope -> "?")
                .collect(Collectors.joining(",", "(", ")"));
    }

    private static String buildDownsamplingQuery(MetricQuery query, String metricsTableName, String scopeFilter, String metricFilter, String tagWhereClause, String aliasedMapCols, String groupByOrdinals)
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
        // If no tags are specified, we need to downsample before aggregating.
        // Specifying 'tags' groupBy here makes it so we don't aggregate.
        // But if downsampler=aggregator, we *do* want the aggregating to happen, as an optimization to do everything in one query
        if (!query.getDownsampler().equals(query.getAggregator()) && isNullOrEmpty(aliasedMapCols)) {
            aliasedMapCols = "tags,";
            groupByOrdinals = "1,";
        }
        String downsamplingSql = MessageFormat.format(
                "SELECT {2} {0}(value) value, scope, metric, {1} time "
                        + "FROM {3} "
                        + "WHERE time >= {8} AND time < {9} {6} {7} {4} "
                        + "GROUP BY {5} scope, metric, {1} ",
                downsamplingAgg,
                timeDownsampleFunction,
                aliasedMapCols,
                metricsTableName,
                tagWhereClause,
                groupByOrdinals,
                scopeFilter,
                metricFilter,
                startTimeParameter,
                endTimeParameter);
        return downsamplingSql;
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

    public enum Property
    {
        /**
         * The TSDB read endpoint.
         */
        PRESTO_JDBC_URL("service.property.tsdb.presto.jdbc.url", "jdbc:presto://localhost:8080"),
        PRESTO_CATALOG_NAME("service.property.tsdb.presto.catalog.name", "hive"),
        PRESTO_SCHEMA_NAME("service.property.tsdb.presto.schema.name", "s3"),
        PRESTO_TABLE_NAME("service.property.tsdb.presto.table.name", "orc_flat_metrics"),
        PRESTO_USER_NAME("service.property.tsdb.presto.user.name", "vincent.poon");

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
