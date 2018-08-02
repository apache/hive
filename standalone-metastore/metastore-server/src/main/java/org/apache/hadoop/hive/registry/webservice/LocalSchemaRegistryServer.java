/**
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.hadoop.hive.registry.webservice;

import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.setup.Environment;
import org.apache.hadoop.hive.registry.webservice.RegistryApplication;
import org.apache.hadoop.hive.registry.webservice.RegistryConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents local instance of SchemaRegistry which can be run in the same JVM like below.
 * <p>
 * <blockquote>
 * <pre>
 * {@code
 * String configPath = new File("schema-registry.yaml").getAbsolutePath();
 * LocalSchemaRegistryServer localSchemaRegistryServer = new LocalSchemaRegistryServer(configPath);
 * 
 * try {
 *      localSchemaRegistryServer.start();
 *      SchemaRegistryClient schemaRegistryClient = createSchemaRegistryClient(localSchemaRegistryServer.getLocalPort());
 *
 *      // registering schema metadata
 *      SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("foo").type("avro").build();
 *      boolean success = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
 *
 *      // registering a new schema
 *      String schemaName = schemaMetadata.getName();
 *      String schema1 = IOUtils.toString(LocalRegistryServerTest.class.getResourceAsStream("/schema-1.avsc"), "UTF-8");
 *      Integer v1 = schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(schema1, "Initial version of the schema"));
 * } finally {
 *  localSchemaRegistryServer.stop();
 * }
 * }
 * </pre>
 * </blockquote>
 */
public class LocalSchemaRegistryServer {

    private final LocalRegistryApplication registryApplication;

    /**
     * Creates an instance of {@link LocalSchemaRegistryServer} with the given {@code configFilePath}
     *
     * @param configFilePath Path of the config file for this server which contains all the required configuration.
     */
    public LocalSchemaRegistryServer(String configFilePath) {
        registryApplication = new LocalRegistryApplication(configFilePath);
    }

    /**
     * Starts the server if it is not started yet.
     *
     * @throws Exception
     */
    public void start() throws Exception {
        registryApplication.start();
    }

    public void stop() throws Exception {
        registryApplication.stop();
    }

    public int getLocalPort() {
        return registryApplication.getLocalPort();
    }

    public int getAdminPort() {
        return registryApplication.getAdminPort();
    }

    public String getLocalURL() {
        return registryApplication.localServer.getURI().toString();
    }


    private static final class LocalRegistryApplication extends RegistryApplication {
        private static final Logger LOG = LoggerFactory.getLogger(LocalRegistryApplication.class);

        private final String configFilePath;
        private volatile Server localServer;

        public LocalRegistryApplication(String configFilePath) {
            this.configFilePath = configFilePath;
        }

        @Override
        public void run(RegistryConfiguration registryConfiguration, Environment environment) throws Exception {
            super.run(registryConfiguration, environment);
            environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
                @Override
                public void serverStarted(Server server) {
                    localServer = server;
                    LOG.info("Received callback as server is started :[{}]", server);
                }
            });
        }

        void start() throws Exception {
            if(localServer == null) {
                LOG.info("Local schema registry instance is getting started.");
                run("server", configFilePath);
                LOG.info("Local schema registry instance is started at port [{}]", getLocalPort());
            } else {
                LOG.info("Local schema registry instance is already started at port [{}]", getLocalPort());
            }
        }

        void stop() throws Exception {
            if (localServer != null) {
                localServer.stop();
                localServer = null;
                LOG.info("Local schema registry instance is stopped.");
            } else {
                LOG.info("No local schema registry instance is running to be stopped.");
            }
        }

        int getLocalPort() {
            return ((ServerConnector) localServer.getConnectors()[0]).getLocalPort();
        }

        int getAdminPort() {
            return ((ServerConnector) localServer.getConnectors()[1]).getLocalPort();
        }

    }
}
