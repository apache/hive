package org.apache.hadoop.hive.registry.serdes;

import org.apache.hadoop.hive.registry.client.ISchemaRegistryClient;
import org.apache.hadoop.hive.registry.client.SchemaRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class AbstractSerDes {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSerDes.class);

  protected ISchemaRegistryClient schemaRegistryClient;
  protected boolean initialized = false;
  protected boolean closed = false;

  public AbstractSerDes() {
    this(null);
  }

  public AbstractSerDes(ISchemaRegistryClient schemaRegistryClient) {
    this.schemaRegistryClient = schemaRegistryClient;
  }

  public final void init(Map<String, ?> config) {
    if (closed) {
      throw new IllegalStateException("Closed instance can not be initialized again");
    }
    if (initialized) {
      LOG.info("This instance [{}] is already inited", this);
      return;
    }

    LOG.debug("Initialized with config: [{}]", config);
    if (schemaRegistryClient == null) {
      schemaRegistryClient = new SchemaRegistryClient(config);
    }

    doInit(config);

    initialized = true;
  }

  protected void doInit(Map<String, ?> config) {
  }

  public void close() throws Exception {
    if (closed) {
      LOG.info("This instance [{}] is already closed", this);
      return;
    }
    try {
      if (schemaRegistryClient != null) {
        schemaRegistryClient.close();
      }
    } finally {
      closed = true;
    }
  }

}
