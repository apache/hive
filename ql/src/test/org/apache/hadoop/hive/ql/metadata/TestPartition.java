package org.apache.hadoop.hive.ql.metadata;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

/**
 * Test the partition class.
 */
public class TestPartition extends TestCase {

  private static final String PARTITION_COL = "partcol";
  private static final String PARTITION_VALUE = "value";
  private static final String TABLENAME = "tablename";

  /**
   * Test that the Partition spec is created properly.
   */
  public void testPartition() throws HiveException, URISyntaxException {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("partlocation");

    Partition tp = new Partition();
    tp.setTableName(TABLENAME);
    tp.setSd(sd);

    List<String> values = new ArrayList<String>();
    values.add(PARTITION_VALUE);
    tp.setValues(values);

    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema(PARTITION_COL, "string", ""));

    Table tbl = new Table(TABLENAME);
    tbl.setDataLocation(new URI("tmplocation"));
    tbl.setPartCols(partCols);

    Map<String, String> spec = new org.apache.hadoop.hive.ql.metadata.Partition(tbl, tp).getSpec();
    assertFalse(spec.isEmpty());
    assertEquals(spec.get(PARTITION_COL), PARTITION_VALUE);
  }

}
