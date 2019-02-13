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
package io.prestosql.plugin.phoenix;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.phoenix.MetadataUtil.decomposePrimaryKeyColumn;
import static io.prestosql.plugin.phoenix.MetadataUtil.getFullTableName;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static java.lang.Float.intBitsToFloat;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;
import static org.joda.time.DateTimeZone.UTC;

public class QueryBuilder
{
    private QueryBuilder()
    {
    }

    public static String buildSql(
            PhoenixConnection connection,
            String catalog,
            String schema,
            String table,
            Optional<Set<ColumnHandle>> desiredColumns,
            List<PhoenixColumnHandle> columns,
            TupleDomain<ColumnHandle> tupleDomain)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder().append("SELECT ");
        if (desiredColumns.isPresent() && !desiredColumns.get().isEmpty()) {
            List<PhoenixColumnHandle> desiredPCols = desiredColumns.get().stream().map(ch -> (PhoenixColumnHandle) ch).collect(Collectors.toList());
            String columnNames = decomposePrimaryKeyColumn(desiredPCols).stream().map(PhoenixColumnHandle::getColumnName).collect(joining(", "));
            sql.append(columnNames);
        }
        else {
            String columnNames = decomposePrimaryKeyColumn(columns).stream().map(PhoenixColumnHandle::getColumnName).collect(joining(", "));
            sql.append(columnNames);
            if (columns.isEmpty()) {
                sql.append("null");
            }
        }
        sql.append(" FROM ");
        sql.append(getFullTableName(catalog, schema, table));

        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> clauses = toConjuncts(columns, tupleDomain, accumulator);
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ").append(Joiner.on(" AND ").join(clauses));
        }

        try (PhoenixPreparedStatement statement = connection.prepareStatement(sql.toString()).unwrap(PhoenixPreparedStatement.class)) {
            for (int i = 0; i < accumulator.size(); i++) {
                TypeAndValue typeAndValue = accumulator.get(i);
                Type type = typeAndValue.getType();
                if (type.equals(BigintType.BIGINT)) {
                    statement.setLong(i + 1, (long) typeAndValue.getValue());
                }
                else if (type.equals(IntegerType.INTEGER)) {
                    statement.setInt(i + 1, ((Number) typeAndValue.getValue()).intValue());
                }
                else if (type.equals(SmallintType.SMALLINT)) {
                    statement.setShort(i + 1, ((Number) typeAndValue.getValue()).shortValue());
                }
                else if (type.equals(TinyintType.TINYINT)) {
                    statement.setByte(i + 1, ((Number) typeAndValue.getValue()).byteValue());
                }
                else if (type.equals(DoubleType.DOUBLE)) {
                    statement.setDouble(i + 1, (double) typeAndValue.getValue());
                }
                else if (type.equals(RealType.REAL)) {
                    statement.setFloat(i + 1, intBitsToFloat(((Number) typeAndValue.getValue()).intValue()));
                }
                else if (type.equals(BooleanType.BOOLEAN)) {
                    statement.setBoolean(i + 1, (boolean) typeAndValue.getValue());
                }
                else if (type.equals(DateType.DATE)) {
                    long millis = DAYS.toMillis((long) typeAndValue.getValue());
                    statement.setDate(i + 1, new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis)));
                }
                else if (type.equals(TimeType.TIME)) {
                    statement.setTime(i + 1, new Time((long) typeAndValue.getValue()));
                }
                else if (type.equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE)) {
                    statement.setTime(i + 1, new Time(unpackMillisUtc((long) typeAndValue.getValue())));
                }
                else if (type.equals(TimestampType.TIMESTAMP)) {
                    statement.setTimestamp(i + 1, new Timestamp((long) typeAndValue.getValue()));
                }
                else if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
                    statement.setTimestamp(i + 1, new Timestamp(unpackMillisUtc((long) typeAndValue.getValue())));
                }
                else if (type instanceof VarcharType || type instanceof CharType) {
                    statement.setString(i + 1, ((Slice) typeAndValue.getValue()).toStringUtf8());
                }
                else if (isShortDecimal(type)) {
                    int scale = ((DecimalType) type).getScale();
                    BigInteger unscaledValue = BigInteger.valueOf((long) typeAndValue.getValue());
                    statement.setBigDecimal(i + 1, new BigDecimal(unscaledValue, scale));
                }
                else if (isLongDecimal(type)) {
                    int scale = ((DecimalType) type).getScale();
                    BigInteger unscaledValue = Decimals.decodeUnscaledValue((Slice) typeAndValue.getValue());
                    statement.setBigDecimal(i + 1, new BigDecimal(unscaledValue, scale));
                }
                else {
                    throw new UnsupportedOperationException("Can't handle type: " + type);
                }
            }
            return generateActualSql(statement.toString(), statement.getParameters().toArray());
        }
    }

    private static boolean isAcceptedType(Type type)
    {
        Type validType = requireNonNull(type, "type is null");
        return validType.equals(BigintType.BIGINT) ||
                validType.equals(TinyintType.TINYINT) ||
                validType.equals(SmallintType.SMALLINT) ||
                validType.equals(IntegerType.INTEGER) ||
                validType.equals(DoubleType.DOUBLE) ||
                validType.equals(RealType.REAL) ||
                validType.equals(BooleanType.BOOLEAN) ||
                validType.equals(DateType.DATE) ||
                validType.equals(TimeType.TIME) ||
                validType.equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE) ||
                validType.equals(TimestampType.TIMESTAMP) ||
                validType.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE) ||
                validType instanceof VarcharType ||
                validType instanceof CharType ||
                validType instanceof DecimalType;
    }

    private static List<String> toConjuncts(List<PhoenixColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, List<TypeAndValue> accumulator)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (PhoenixColumnHandle column : columns) {
            Type type = column.getColumnType();
            if (isAcceptedType(type)) {
                Domain domain = tupleDomain.getDomains().get().get(column);
                if (domain != null) {
                    builder.add(toPredicate(column.getColumnName(), domain, type, accumulator));
                }
            }
        }
        return builder.build();
    }

    private static String toPredicate(String columnName, Domain domain, Type type, List<TypeAndValue> accumulator)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? columnName + " IS NULL" : "FALSE";
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? "TRUE" : columnName + " IS NOT NULL";
        }

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type, accumulator));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type, accumulator));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type, accumulator));
                            break;
                        case BELOW:
                            rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type, accumulator));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(columnName, "=", getOnlyElement(singleValues), type, accumulator));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(value, type, accumulator);
            }
            String values = Joiner.on(",").join(nCopies(singleValues.size(), "?"));
            disjuncts.add(columnName + " IN (" + values + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(columnName + " IS NULL");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private static String toPredicate(String columnName, String operator, Object value, Type type, List<TypeAndValue> accumulator)
    {
        bindValue(value, type, accumulator);
        return columnName + " " + operator + " ?";
    }

    private static void bindValue(Object value, Type type, List<TypeAndValue> accumulator)
    {
        checkArgument(isAcceptedType(type), "Can't handle type: %s", type);
        accumulator.add(new TypeAndValue(type, value));
    }

    private static String generateActualSql(String sqlQuery, Object... parameters)
    {
        String[] parts = sqlQuery.split("\\?");
        StringBuilder sb = new StringBuilder();

        // This might be wrong if some '?' are used as litteral '?'
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            sb.append(part);
            if (i < parameters.length) {
                sb.append(formatParameter(parameters[i]));
            }
        }

        return sb.toString();
    }

    private static String formatParameter(Object parameter)
    {
        if (parameter == null) {
            return "NULL";
        }
        else {
            if (parameter instanceof String) {
                return "'" + ((String) parameter).replace("'", "''") + "'";
            }
            else if (parameter instanceof Timestamp) {
                return "to_timestamp('" + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS").format(parameter) + "', 'MM/dd/yyyy HH:mm:ss.SSS')";
            }
            else if (parameter instanceof Date) {
                return "to_date('" + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(parameter) + "', 'MM/dd/yyyy HH:mm:ss')";
            }
            else {
                return parameter.toString();
            }
        }
    }

    private static class TypeAndValue
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
}
