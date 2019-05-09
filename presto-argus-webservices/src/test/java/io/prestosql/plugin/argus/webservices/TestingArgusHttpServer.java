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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.server.TheServlet;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.testing.TestingNodeModule;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.System.lineSeparator;
import static java.util.stream.Collectors.joining;

public class TestingArgusHttpServer
{
    private static final ObjectMapper objectMapper = new ObjectMapperProvider().get();

    private final LifeCycleManager lifeCycleManager;
    private final URI baseUri;

    private final Injector injector;

    public TestingArgusHttpServer()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                new ArgusHttpServerModule());

        injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        baseUri = injector.getInstance(TestingHttpServer.class).getBaseUrl();
    }

    public void stop()
            throws Exception
    {
        lifeCycleManager.stop();
    }

    public URI getBaseUri()
    {
        return baseUri;
    }

    public void loadRequestResponses(String resourceName)
    {
        ArgusHttpServlet argusServlet = (ArgusHttpServlet) injector.getInstance(Key.get(Servlet.class, TheServlet.class));
        argusServlet.loadRequestResponses(resourceName);
    }

    public URI resolve(String s)
    {
        return baseUri.resolve(s);
    }

    private static class ArgusHttpServerModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(TheServlet.class).toInstance(ImmutableMap.of());
            binder.bind(Servlet.class).annotatedWith(TheServlet.class).toInstance(new ArgusHttpServlet());
        }
    }

    @SuppressWarnings("serial")
    static class ArgusHttpServlet
            extends HttpServlet
    {
        private final Map<HttpRequest, HttpRequestResponse> requestResponses = new HashMap<>();

        @Override
        public void init()
                throws ServletException
        {
            super.init();
            loadRequestResponses("/AuthService.json");
        }

        public void loadRequestResponses(String resourceName)
        {
            try {
                HttpRequestResponse[] staticResponses = objectMapper.readValue(Resources.getResource(TestingArgusHttpServer.class, resourceName), HttpRequestResponse[].class);
                for (HttpRequestResponse requestResponse : staticResponses) {
                    HttpRequest request = new HttpRequest(requestResponse.getType(), requestResponse.getEndpoint(), requestResponse.getJsonInput());
                    requestResponses.put(request, requestResponse);
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            String pathWithQuery = request.getPathInfo() + "?" + URLDecoder.decode(request.getQueryString(), "UTF-8");
            doRequestResponse(response, request.getMethod(), pathWithQuery, null);
        }

        @Override
        protected void doPost(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException
        {
            String body = request.getReader().lines().collect(joining(lineSeparator()));
            doRequestResponse(response, request.getMethod(), request.getPathInfo(), body);
        }

        private void doRequestResponse(HttpServletResponse response,
                String method, String path, String body)
                throws IOException
        {
            HttpRequest httpRequest = new HttpRequest(method, path, body);
            HttpRequestResponse httpRequestResponse = requestResponses.get(httpRequest);
            if (httpRequestResponse == null) {
                throw new IllegalArgumentException("No defined response found for request: " + httpRequest);
            }
            response.setContentType("application/json");
            if (httpRequestResponse.getStatus() == HttpStatus.OK.code()) {
                response.setStatus(HttpStatus.OK.code());
            }
            else {
                response.sendError(httpRequestResponse.getStatus(), httpRequestResponse.getMessage());
            }
            response.getWriter().print(httpRequestResponse.getJsonOutput());
            response.flushBuffer();
        }
    }

    public static class HttpRequest
    {
        protected String type;
        protected String endpoint;
        protected String jsonInput;

        public HttpRequest() {}

        @JsonCreator
        public HttpRequest(@JsonProperty String type, @JsonProperty String endpoint, @JsonProperty String jsonInput)
        {
            this.type = type;
            this.endpoint = endpoint;
            this.jsonInput = jsonInput;
        }

        @JsonProperty
        public String getType()
        {
            return type;
        }

        public void setType(String type)
        {
            this.type = type;
        }

        @JsonProperty
        public String getEndpoint()
        {
            return endpoint;
        }

        public void setEndpoint(String endpoint)
        {
            this.endpoint = endpoint;
        }

        @JsonProperty
        public String getJsonInput()
        {
            return jsonInput;
        }

        public void setJsonInput(String jsonInput)
        {
            this.jsonInput = jsonInput;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            HttpRequest other = (HttpRequest) obj;
            return Objects.equals(endpoint, other.endpoint)
                    && Objects.equals(jsonInput, other.jsonInput)
                    && Objects.equals(type, other.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(endpoint, jsonInput, type);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .add("endpoint", endpoint)
                    .add("jsonInput", jsonInput)
                    .toString();
        }
    }

    public static class HttpRequestResponse
            extends HttpRequest
    {
        private int status;
        private String message;
        private String jsonOutput;

        @JsonCreator
        public HttpRequestResponse(
                @JsonProperty String type,
                @JsonProperty String endpoint,
                @JsonProperty String jsonInput,
                @JsonProperty int status,
                @JsonProperty String message,
                @JsonProperty String jsonOutput)
        {
            super(type, endpoint, jsonInput);
            this.status = status;
            this.message = message;
            this.jsonOutput = jsonOutput;
        }

        @JsonProperty
        public int getStatus()
        {
            return status;
        }

        public void setStatus(int status)
        {
            this.status = status;
        }

        @JsonProperty
        public String getMessage()
        {
            return message;
        }

        public void setMessage(String message)
        {
            this.message = message;
        }

        @JsonProperty
        public String getJsonOutput()
        {
            return jsonOutput;
        }

        public void setJsonOutput(String jsonOutput)
        {
            this.jsonOutput = jsonOutput;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("request", super.toString())
                    .add("status", status)
                    .add("jsonOutput", jsonOutput)
                    .toString();
        }
    }
}
