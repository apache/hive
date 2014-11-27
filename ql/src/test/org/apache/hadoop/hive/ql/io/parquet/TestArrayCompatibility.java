package org.apache.hadoop.hive.ql.io.parquet;

import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.Types;

import static parquet.schema.OriginalType.LIST;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class TestArrayCompatibility extends AbstractTestParquetDirect {

  @Test
  public void testUnannotatedListOfPrimitives() throws Exception {
    MessageType fileSchema = Types.buildMessage()
        .repeated(INT32).named("list_of_ints")
        .named("UnannotatedListOfPrimitives");

    Path test = writeDirect("UnannotatedListOfPrimitives",
        fileSchema,
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("list_of_ints", 0);

            rc.addInteger(34);
            rc.addInteger(35);
            rc.addInteger(36);

            rc.endField("list_of_ints", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        new IntWritable(34), new IntWritable(35), new IntWritable(36)));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));
  }

  @Test
  public void testUnannotatedListOfGroups() throws Exception {
    Path test = writeDirect("UnannotatedListOfGroups",
        Types.buildMessage()
            .repeatedGroup()
                .required(FLOAT).named("x")
                .required(FLOAT).named("y")
                .named("list_of_points")
            .named("UnannotatedListOfGroups"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("list_of_points", 0);

            rc.startGroup();
            rc.startField("x", 0);
            rc.addFloat(1.0f);
            rc.endField("x", 0);
            rc.startField("y", 1);
            rc.addFloat(1.0f);
            rc.endField("y", 1);
            rc.endGroup();

            rc.startGroup();
            rc.startField("x", 0);
            rc.addFloat(2.0f);
            rc.endField("x", 0);
            rc.startField("y", 1);
            rc.addFloat(2.0f);
            rc.endField("y", 1);
            rc.endGroup();

            rc.endField("list_of_points", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new FloatWritable(1.0f), new FloatWritable(1.0f)),
        record(new FloatWritable(2.0f), new FloatWritable(2.0f))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));
  }

  @Test
  public void testThriftPrimitiveInList() throws Exception {
    Path test = writeDirect("ThriftPrimitiveInList",
        Types.buildMessage()
            .requiredGroup().as(LIST)
                .repeated(INT32).named("list_of_ints_tuple")
                .named("list_of_ints")
            .named("ThriftPrimitiveInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("list_of_ints", 0);

            rc.startGroup();
            rc.startField("list_of_ints_tuple", 0);

            rc.addInteger(34);
            rc.addInteger(35);
            rc.addInteger(36);

            rc.endField("list_of_ints_tuple", 0);
            rc.endGroup();

            rc.endField("list_of_ints", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        new IntWritable(34), new IntWritable(35), new IntWritable(36)));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record", expected, records.get(0));
  }

  @Test
  public void testThriftSingleFieldGroupInList() throws Exception {
    // this tests the case where older data has an ambiguous structure, but the
    // correct interpretation can be determined from the repeated name

    Path test = writeDirect("ThriftSingleFieldGroupInList",
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .required(INT64).named("count")
                    .named("single_element_groups_tuple")
                .named("single_element_groups")
            .named("ThriftSingleFieldGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("single_element_groups", 0);

            rc.startGroup();
            rc.startField("single_element_groups_tuple", 0); // start writing array contents

            rc.startGroup();
            rc.startField("count", 0);
            rc.addLong(1234L);
            rc.endField("count", 0);
            rc.endGroup();

            rc.startGroup();
            rc.startField("count", 0);
            rc.addLong(2345L);
            rc.endField("count", 0);
            rc.endGroup();

            rc.endField("single_element_groups_tuple", 0); // finished writing array contents
            rc.endGroup();

            rc.endField("single_element_groups", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new LongWritable(1234L)),
        record(new LongWritable(2345L))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));
  }

  @Test
  public void testAvroPrimitiveInList() throws Exception {
    Path test = writeDirect("AvroPrimitiveInList",
        Types.buildMessage()
            .requiredGroup().as(LIST)
                .repeated(INT32).named("array")
                .named("list_of_ints")
            .named("AvroPrimitiveInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("list_of_ints", 0);

            rc.startGroup();
            rc.startField("array", 0);

            rc.addInteger(34);
            rc.addInteger(35);
            rc.addInteger(36);

            rc.endField("array", 0);
            rc.endGroup();

            rc.endField("list_of_ints", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        new IntWritable(34), new IntWritable(35), new IntWritable(36)));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record", expected, records.get(0));
  }

  @Test
  public void testAvroSingleFieldGroupInList() throws Exception {
    // this tests the case where older data has an ambiguous structure, but the
    // correct interpretation can be determined from the repeated name, "array"

    Path test = writeDirect("AvroSingleFieldGroupInList",
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .required(INT64).named("count")
                    .named("array")
                .named("single_element_groups")
            .named("AvroSingleFieldGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("single_element_groups", 0);

            rc.startGroup();
            rc.startField("array", 0); // start writing array contents

            rc.startGroup();
            rc.startField("count", 0);
            rc.addLong(1234L);
            rc.endField("count", 0);
            rc.endGroup();

            rc.startGroup();
            rc.startField("count", 0);
            rc.addLong(2345L);
            rc.endField("count", 0);
            rc.endGroup();

            rc.endField("array", 0); // finished writing array contents
            rc.endGroup();

            rc.endField("single_element_groups", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new LongWritable(1234L)),
        record(new LongWritable(2345L))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));
  }

  @Test
  public void testAmbiguousSingleFieldGroupInList() throws Exception {
    // this tests the case where older data has an ambiguous list and is not
    // named indicating that the source considered the group significant

    Path test = writeDirect("SingleFieldGroupInList",
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .required(INT64).named("count")
                    .named("single_element_group")
                .named("single_element_groups")
            .named("SingleFieldGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("single_element_groups", 0);

            rc.startGroup();
            rc.startField("single_element_group", 0); // start writing array contents

            rc.startGroup();
            rc.startField("count", 0);
            rc.addLong(1234L);
            rc.endField("count", 0);
            rc.endGroup();

            rc.startGroup();
            rc.startField("count", 0);
            rc.addLong(2345L);
            rc.endField("count", 0);
            rc.endGroup();

            rc.endField("single_element_group", 0); // finished writing array contents
            rc.endGroup();

            rc.endField("single_element_groups", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        new LongWritable(1234L),
        new LongWritable(2345L)));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));
  }

  @Test
  public void testMultiFieldGroupInList() throws Exception {
    // tests the missing element layer, detected by a multi-field group

    Path test = writeDirect("MultiFieldGroupInList",
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .required(DOUBLE).named("latitude")
                    .required(DOUBLE).named("longitude")
                    .named("element") // should not affect schema conversion
                .named("locations")
            .named("MultiFieldGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("locations", 0);

            rc.startGroup();
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(0.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(180.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup();

            rc.endField("locations", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new DoubleWritable(0.0), new DoubleWritable(0.0)),
        record(new DoubleWritable(0.0), new DoubleWritable(180.0))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));
  }

  @Test
  public void testNewOptionalGroupInList() throws Exception {
    Path test = writeDirect("NewOptionalGroupInList",
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .optionalGroup()
                        .required(DOUBLE).named("latitude")
                        .required(DOUBLE).named("longitude")
                        .named("element")
                    .named("list")
                .named("locations")
            .named("NewOptionalGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("locations", 0);

            rc.startGroup();
            rc.startField("list", 0); // start writing array contents

            // write a non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(0.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            // write a null element (element field is omitted)
            rc.startGroup(); // array level
            rc.endGroup(); // array level

            // write a second non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(180.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            rc.endField("list", 0); // finished writing array contents
            rc.endGroup();

            rc.endField("locations", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new DoubleWritable(0.0), new DoubleWritable(0.0)),
        null,
        record(new DoubleWritable(0.0), new DoubleWritable(180.0))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));
  }

  @Test
  public void testNewRequiredGroupInList() throws Exception {
    Path test = writeDirect("NewRequiredGroupInList",
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .requiredGroup()
                        .required(DOUBLE).named("latitude")
                        .required(DOUBLE).named("longitude")
                        .named("element")
                    .named("list")
                .named("locations")
            .named("NewRequiredGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("locations", 0);

            rc.startGroup();
            rc.startField("list", 0); // start writing array contents

            // write a non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(180.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            // write a second non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(0.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            rc.endField("list", 0); // finished writing array contents
            rc.endGroup();

            rc.endField("locations", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new DoubleWritable(0.0), new DoubleWritable(180.0)),
        record(new DoubleWritable(0.0), new DoubleWritable(0.0))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));
  }

  @Test
  public void testHiveRequiredGroupInList() throws Exception {
    // this matches the list structure that Hive writes
    Path test = writeDirect("HiveRequiredGroupInList",
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .requiredGroup()
                        .required(DOUBLE).named("latitude")
                        .required(DOUBLE).named("longitude")
                        .named("element")
                    .named("bag")
                .named("locations")
            .named("HiveRequiredGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("locations", 0);

            rc.startGroup();
            rc.startField("bag", 0); // start writing array contents

            // write a non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(180.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            // write a second non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(0.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            rc.endField("bag", 0); // finished writing array contents
            rc.endGroup();

            rc.endField("locations", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new DoubleWritable(0.0), new DoubleWritable(180.0)),
        record(new DoubleWritable(0.0), new DoubleWritable(0.0))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));
  }

}
