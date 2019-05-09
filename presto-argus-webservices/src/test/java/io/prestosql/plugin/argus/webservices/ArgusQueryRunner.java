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

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;

import java.util.Map;

import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class ArgusQueryRunner
{
    private ArgusQueryRunner()
    {
    }

    public static QueryRunner createArgusQueryRunner(TestingArgusHttpServer server)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession()).build();

        queryRunner.installPlugin(new ArgusWebServicesPlugin());
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(ArgusWebServicesConfig.ENDPOINT, server.getBaseUri().toString())
                .put(ArgusWebServicesConfig.USERNAME, "aUsername")
                .put(ArgusWebServicesConfig.PASSWORD, "aPassword")
                .build();
        queryRunner.createCatalog("argus_webservices", "argus_webservices", properties);

        return queryRunner;
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("argus_webservices")
                .build();
    }
}
