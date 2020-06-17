package org.apache.hadoop.hive.metastore.annotation;

/**
 * Marker interface for tests run against a Catalog service which exposes a HMS interface.
 * Currently, assumes that such a CatalogService is running on host provided by
 * {@code CATALOG_SERVICE_HOST} and port provided by {@code CATALOG_HMS_SERVICE_PORT}
 * system properties.
 */
public interface RemoteMetastoreUnitTest extends MetastoreTest {
}
