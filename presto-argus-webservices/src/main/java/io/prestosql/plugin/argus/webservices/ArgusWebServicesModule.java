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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.argus.ArgusClient;
import io.prestosql.plugin.argus.ArgusConnector;
import io.prestosql.plugin.argus.ArgusMetadata;
import io.prestosql.plugin.argus.ArgusPageSourceProvider;
import io.prestosql.plugin.argus.ArgusRecordSetProvider;
import io.prestosql.plugin.argus.ArgusSessionProperties;
import io.prestosql.spi.connector.ConnectorSplitManager;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ArgusWebServicesModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ArgusConnector.class).in(Scopes.SINGLETON);
        binder.bind(ArgusMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ArgusClient.class).to(ArgusWebServicesClient.class).in(Scopes.SINGLETON);
        binder.bind(ArgusRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(ArgusWebServicesSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ArgusPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ArgusSessionProperties.class).to(ArgusWebServicesSessionProperties.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ArgusWebServicesConfig.class);
    }
}
