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
package io.prestosql.plugin.argus;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

public class ArgusSessionProperties
{
    @VisibleForTesting
    static final String TIME_RANGE_SPLITS = "time_range_splits";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ArgusSessionProperties()
    {
        sessionProperties = ImmutableList.of(
                PropertyMetadata.integerProperty(
                        TIME_RANGE_SPLITS,
                        "Number of splits of the query time range, to be executed in parallel",
                        1,
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Integer getTimeRangeSplits(ConnectorSession session)
    {
        return session.getProperty(TIME_RANGE_SPLITS, Integer.class);
    }
}
