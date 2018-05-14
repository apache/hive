package org.apache.hadoop.hive.registry.conf;

import com.google.common.io.Resources;

import java.io.File;
import java.net.URISyntaxException;

public class SchemaRegistryTestConfiguration {
  private String serverYAMLPath;
  private String clientYAMLPath;

  public SchemaRegistryTestConfiguration(String serverYAMLPath, String clientYAMLPath) {
    this.serverYAMLPath = serverYAMLPath;
    this.clientYAMLPath = clientYAMLPath;
  }

  public static SchemaRegistryTestConfiguration forProfileType(SchemaRegistryTestProfileType testProfileType) throws URISyntaxException {
    String serverYAMLFileName;
    String clientYAMLFileName;
    switch (testProfileType) {
      case DEFAULT:
        serverYAMLFileName = "schema-registry-test.yaml";
        clientYAMLFileName = "schema-registry-client.yaml";
        break;
      case SSL:
        serverYAMLFileName = "ssl-schema-registry-test.yaml";
        clientYAMLFileName = "ssl-schema-registry-client.yaml";
        break;
      case DEFAULT_HA:
        serverYAMLFileName = "schema-registry-test-ha.yaml";
        clientYAMLFileName = null;
        break;
      case SSL_HA:
        serverYAMLFileName = "ssl-schema-registry-test-ha.yaml";
        clientYAMLFileName = "ssl-schema-registry-client.yaml";
        break;
      default:
        throw new IllegalArgumentException("Unrecognized SchemaRegistryTestProfileType : " + testProfileType);
    }

    String serverYAMLPath = new File(Resources.getResource(serverYAMLFileName).toURI()).getAbsolutePath();
    String clientYAMLPath = clientYAMLFileName == null ? null : new File(Resources.getResource(clientYAMLFileName).toURI()).getAbsolutePath();

    return new SchemaRegistryTestConfiguration(serverYAMLPath, clientYAMLPath);
  }

  public String getServerYAMLPath() {
    return this.serverYAMLPath;
  }

  public String getClientYAMLPath() {
    return this.clientYAMLPath;
  }

}

