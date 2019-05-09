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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ArgusWebServicesConfig
{
    public static final String ENDPOINT = "argus.endpoint";
    public static final String USERNAME = "argus.username";
    public static final String PASSWORD = "argus.password";

    private String endpoint;
    private String username;
    private String password;
    private int maxConnections = 10;
    private Duration connectionTimeout = new Duration(10, SECONDS);
    private Duration requestTimeout = new Duration(30, SECONDS);

    @NotNull
    public String getEndpoint()
    {
        return endpoint;
    }

    @Config(ENDPOINT)
    public ArgusWebServicesConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @NotNull
    public String getUsername()
    {
        return username;
    }

    @Config(USERNAME)
    public void setUsername(String username)
    {
        this.username = username;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @Config(PASSWORD)
    public ArgusWebServicesConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @Min(1)
    public int getMaxConnections()
    {
        return maxConnections;
    }

    @Config("argus.max-connections")
    public ArgusWebServicesConfig setMaxConnections(int connections)
    {
        this.maxConnections = connections;
        return this;
    }

    @MinDuration("1ms")
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("argus.connection-timeout")
    public ArgusWebServicesConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @MinDuration("1ms")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("argus.request-timeout")
    public ArgusWebServicesConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }
}
