package org.apache.hadoop.hive.registry.helper;

import org.apache.hadoop.hive.registry.conf.SchemaRegistryTestConfiguration;
import org.apache.hadoop.hive.registry.conf.SchemaRegistryTestProfileType;
import org.apache.hadoop.hive.registry.webservice.LocalSchemaRegistryServer;
import org.apache.hadoop.hive.registry.client.SchemaRegistryClient;

import org.apache.commons.io.IOUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class SchemaRegistryTestServerClientWrapper {

  private LocalSchemaRegistryServer localSchemaRegistryServer;
  private SchemaRegistryTestConfiguration schemaRegistryTestConfiguration;
  private volatile Map<String, Object> cachedClientConf;
  private volatile SchemaRegistryClient cachedSchemaRegistryClient;
  private static final String V1_API_PATH = "api/v1";

  public SchemaRegistryTestServerClientWrapper(SchemaRegistryTestConfiguration schemaRegistryTestConfiguration) throws URISyntaxException {
    this.schemaRegistryTestConfiguration = schemaRegistryTestConfiguration;
    localSchemaRegistryServer = new LocalSchemaRegistryServer(schemaRegistryTestConfiguration.getServerYAMLPath());
  }

  public SchemaRegistryTestServerClientWrapper(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws URISyntaxException {
    this(SchemaRegistryTestConfiguration.forProfileType(schemaRegistryTestProfileType));
  }

  public void startTestServer() throws Exception {
    try {
      localSchemaRegistryServer.start();
    } catch (Exception e) {
      localSchemaRegistryServer.stop();
      throw e;
    }
  }

  public int getLocalPort() {
    return this.localSchemaRegistryServer.getLocalPort();
  }

  public int getAdminPort() {
    return this.localSchemaRegistryServer.getAdminPort();
  }

  public void stopTestServer() throws Exception {
    localSchemaRegistryServer.stop();
  }

  public SchemaRegistryClient getClient() throws IOException {
    return getClient(false);
  }

  public SchemaRegistryClient getClient(boolean cached) throws IOException {
    if (!cached) {
      SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(exportClientConf(false));
      if (cachedSchemaRegistryClient == null) {
        cachedSchemaRegistryClient = schemaRegistryClient;
      }
      return schemaRegistryClient;
    } else {
      if (cachedSchemaRegistryClient == null) {
        cachedSchemaRegistryClient = new SchemaRegistryClient(exportClientConf(true));
      }
      return cachedSchemaRegistryClient;
    }
  }

  public Map<String, Object> exportClientConf() {
    return exportClientConf(false);
  }

  public Map<String, Object> exportClientConf(boolean cached) {
    if (!cached) {
      Map<String, Object> clientConfig = createClientConf();
      if (cachedClientConf == null) {
        cachedClientConf = clientConfig;
      }
      return clientConfig;
    } else {
      if (cachedClientConf == null) {
        cachedClientConf = createClientConf();
      }
      return cachedClientConf;
    }
  }

  private Map<String, Object> createClientConf() {
    String registryURL = localSchemaRegistryServer.getLocalURL() + V1_API_PATH;
    if (schemaRegistryTestConfiguration.getClientYAMLPath() == null) {
      Map<String, Object> ret = new HashMap<>();
      ret.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), registryURL);
      return ret;
    }
    try (FileInputStream fis = new FileInputStream(schemaRegistryTestConfiguration.getClientYAMLPath())) {
      Map<String, Object> ret = (Map<String, Object>) new Yaml().load(IOUtils.toString(fis, "UTF-8"));
      ret.put("schema.registry.url", registryURL);
      return ret;
    } catch(Exception e) {
      throw new RuntimeException("Failed to export schema client configuration for yaml : " + schemaRegistryTestConfiguration.getClientYAMLPath(), e);
    }
  }
}
