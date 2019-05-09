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
package io.prestosql.plugin.argus.webservices;

import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static java.lang.String.format;

public class TestArgusIntegrationSmokeTest
        extends AbstractTestQueryFramework
{
    public TestArgusIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingArgusHttpServer());
    }

    public TestArgusIntegrationSmokeTest(TestingArgusHttpServer argusHttpServer)
    {
        super(() -> ArgusQueryRunner.createArgusQueryRunner(argusHttpServer));
        this.argusHttpServer = argusHttpServer;
        argusHttpServer.loadRequestResponses("/TestArgusIntegrationSmoke.json");
    }

    private final TestingArgusHttpServer argusHttpServer;

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (argusHttpServer != null) {
            argusHttpServer.stop();
        }
    }

    @Test
    public void testDiscovery()
    {
        assertQuery("SELECT scope, metric, tag_key, tag_value FROM argus_webservices.system.discovery", "SELECT 'aScope', 'aMetric', 'aTagK', 'aTagV'");
        assertQuery("SELECT count(*) FROM argus_webservices.system.discovery WHERE scope_regex = 'multipart.scope.*'", "SELECT 2");
        assertQuery("SELECT tag_key FROM argus_webservices.system.discovery WHERE scope_regex = 'multipart.scope.*'", "SELECT 'source' UNION SELECT 'device'");
        assertQuery("SELECT tag_key FROM argus_webservices.system.discovery WHERE scope_regex = 'multipart.scope.*' LIMIT 1", "SELECT 'source'");
    }

    @Test
    public void testMetricsQuery()
    {
        String mapQuery = "SELECT datapoints[%s] FROM argus_webservices.system.mapped_metrics "
                + "WHERE start_time = timestamp '2019-06-18 19:45:00.000 UTC' "
                + "AND scope = 'multipart.scope.first' "
                + "AND metric = 'some.metric.one' "
                + "AND aggregator = 'avg'";
        assertMapValue(mapQuery, "timestamp '2019-06-18 19:45:00.000 UTC'", "123.456");
        assertMapValue(mapQuery, "timestamp '2019-06-18 19:45:00.001 UTC'", "234.567");
        assertMapValue(mapQuery, "timestamp '2019-06-18 19:45:00.002 UTC'", "345.678");

        String tagsQuery = "SELECT tags[%s] "
                + "FROM argus_webservices.system.mapped_metrics "
                + "WHERE start_time = timestamp '2019-06-18 19:45:00.000 UTC' "
                + "AND scope = 'multipart.scope.first' "
                + "AND metric = 'some.metric.one' "
                + "AND aggregator = 'avg' "
                + "AND tags = map(array['device', 'source'], array['*', '*']) "
                + "AND downsampler = '1m-max'";
        assertMapValue(tagsQuery, "'device'", "'testDeviceA'");
        assertMapValue(tagsQuery, "'source'", "'testSource'");

        String multipleResults = "SELECT count(*) FROM argus_webservices.system.mapped_metrics "
                + "WHERE start_time = timestamp '2019-06-18 19:45:00.000 UTC' "
                + "AND scope = 'multipart.scope.first' "
                + "AND metric = 'multiple.results' "
                + "AND aggregator = 'avg'";
        assertQuery(multipleResults, "SELECT 2");
    }

    @Test
    public void testExpressionQuery()
    {
        String mapQuery = "SELECT datapoints[%s] FROM argus_webservices.system.mapped_metrics "
                + "WHERE expression = '1560887100000:multipart.scope.first:some.metric.one:avg'";
        assertMapValue(mapQuery, "timestamp '2019-06-18 19:45:00.000 UTC'", "123.456");
        assertMapValue(mapQuery, "timestamp '2019-06-18 19:45:00.001 UTC'", "234.567");
        assertMapValue(mapQuery, "timestamp '2019-06-18 19:45:00.002 UTC'", "345.678");
    }

    private void assertMapValue(String query, String key, String value)
    {
        assertQuery(format(query, key), "select " + value);
    }
}
