package org.apache.hadoop.hive.ql.io.parquet;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.Types;

import static parquet.schema.OriginalType.*;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.*;

public class TestMapStructures extends AbstractTestParquetDirect {

  @Test
  public void testStringMapRequiredPrimitive() throws Exception {
    Path test = writeDirect("StringMapRequiredPrimitive",
        Types.buildMessage()
            .optionalGroup().as(MAP)
                .repeatedGroup()
                    .required(BINARY).as(UTF8).named("key")
                    .required(INT32).named("value")
                    .named("key_value")
                .named("votes")
            .named("StringMapRequiredPrimitive"),
        new TestArrayCompatibility.DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("votes", 0);

            rc.startGroup();
            rc.startField("key_value", 0);

            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("lettuce"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.addInteger(34);
            rc.endField("value", 1);
            rc.endGroup();

            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("cabbage"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.addInteger(18);
            rc.endField("value", 1);
            rc.endGroup();

            rc.endField("key_value", 0);
            rc.endGroup();

            rc.endField("votes", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new Text("lettuce"), new IntWritable(34)),
        record(new Text("cabbage"), new IntWritable(18))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));

    deserialize(records.get(0),
        Arrays.asList("votes"),
        Arrays.asList("map<string,int>"));
  }

  @Test
  public void testStringMapOptionalPrimitive() throws Exception {
    Path test = writeDirect("StringMapOptionalPrimitive",
        Types.buildMessage()
            .optionalGroup().as(MAP)
                .repeatedGroup()
                    .required(BINARY).as(UTF8).named("key")
                    .optional(INT32).named("value")
                    .named("key_value")
                .named("votes")
            .named("StringMapOptionalPrimitive"),
        new TestArrayCompatibility.DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("votes", 0);

            rc.startGroup();
            rc.startField("key_value", 0);

            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("lettuce"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.addInteger(34);
            rc.endField("value", 1);
            rc.endGroup();

            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("kale"));
            rc.endField("key", 0);
            // no value for kale
            rc.endGroup();

            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("cabbage"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.addInteger(18);
            rc.endField("value", 1);
            rc.endGroup();

            rc.endField("key_value", 0);
            rc.endGroup();

            rc.endField("votes", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new Text("lettuce"), new IntWritable(34)),
        record(new Text("kale"), null),
        record(new Text("cabbage"), new IntWritable(18))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));

    deserialize(records.get(0),
        Arrays.asList("votes"),
        Arrays.asList("map<string,int>"));
  }

  @Test
  public void testStringMapOfOptionalArray() throws Exception {
    // tests a multimap structure

    Path test = writeDirect("StringMapOfOptionalArray",
        Types.buildMessage()
            .optionalGroup().as(MAP)
                .repeatedGroup()
                    .required(BINARY).as(UTF8).named("key")
                    .optionalGroup().as(LIST)
                        .repeatedGroup()
                            .optional(BINARY).as(UTF8).named("element")
                            .named("list")
                        .named("value")
                    .named("key_value")
                .named("examples")
            .named("StringMapOfOptionalArray"),
        new TestArrayCompatibility.DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("examples", 0);

            rc.startGroup();
            rc.startField("key_value", 0);

            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("green"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.startGroup();
            rc.startField("list", 0);
            rc.startGroup();
            rc.startField("element", 0);
            rc.addBinary(Binary.fromString("lettuce"));
            rc.endField("element", 0);
            rc.endGroup();
            rc.startGroup();
            rc.startField("element", 0);
            rc.addBinary(Binary.fromString("kale"));
            rc.endField("element", 0);
            rc.endGroup();
            rc.startGroup();
            // adds a null element
            rc.endGroup();
            rc.endField("list", 0);
            rc.endGroup();
            rc.endField("value", 1);
            rc.endGroup();

            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("brown"));
            rc.endField("key", 0);
            // no values array
            rc.endGroup();

            rc.endField("key_value", 0);
            rc.endGroup();

            rc.endField("examples", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new Text("green"), list(new Text("lettuce"), new Text("kale"), null)),
        record(new Text("brown"), null)));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));

    deserialize(records.get(0),
        Arrays.asList("examples"),
        Arrays.asList("map<string,array<string>>"));
  }

  @Test
  public void testStringMapOfOptionalIntArray() throws Exception {
    // tests a multimap structure for PARQUET-26

    Path test = writeDirect("StringMapOfOptionalIntArray",
        Types.buildMessage()
            .optionalGroup().as(MAP)
                .repeatedGroup()
                    .required(BINARY).as(UTF8).named("key")
                        .optionalGroup().as(LIST)
                            .repeatedGroup()
                            .optional(INT32).named("element")
                            .named("list")
                        .named("value")
                    .named("key_value")
                .named("examples")
            .named("StringMapOfOptionalIntArray"),
        new TestArrayCompatibility.DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("examples", 0);

            rc.startGroup();
            rc.startField("key_value", 0);

            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("low"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.startGroup();
            rc.startField("list", 0);
            rc.startGroup();
            rc.startField("element", 0);
            rc.addInteger(34);
            rc.endField("element", 0);
            rc.endGroup();
            rc.startGroup();
            rc.startField("element", 0);
            rc.addInteger(35);
            rc.endField("element", 0);
            rc.endGroup();
            rc.startGroup();
            // adds a null element
            rc.endGroup();
            rc.endField("list", 0);
            rc.endGroup();
            rc.endField("value", 1);
            rc.endGroup();

            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("high"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.startGroup();
            rc.startField("list", 0);
            rc.startGroup();
            rc.startField("element", 0);
            rc.addInteger(340);
            rc.endField("element", 0);
            rc.endGroup();
            rc.startGroup();
            rc.startField("element", 0);
            rc.addInteger(360);
            rc.endField("element", 0);
            rc.endGroup();
            rc.endField("list", 0);
            rc.endGroup();
            rc.endField("value", 1);
            rc.endGroup();

            rc.endField("key_value", 0);
            rc.endGroup();

            rc.endField("examples", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new Text("low"), list(new IntWritable(34), new IntWritable(35), null)),
        record(new Text("high"), list(new IntWritable(340), new IntWritable(360)))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));

    deserialize(records.get(0),
        Arrays.asList("examples"),
        Arrays.asList("map<string,array<int>>"));
  }

  @Test
  public void testMapWithComplexKey() throws Exception {
    Path test = writeDirect("MapWithComplexKey",
        Types.buildMessage()
            .optionalGroup().as(MAP)
                .repeatedGroup()
                    .requiredGroup()
                        .required(INT32).named("x")
                        .required(INT32).named("y")
                        .named("key")
                    .optional(DOUBLE).named("value")
                    .named("key_value")
                .named("matrix")
            .named("MapWithComplexKey"),
        new TestArrayCompatibility.DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("matrix", 0);

            rc.startGroup();
            rc.startField("key_value", 0);

            rc.startGroup();
            rc.startField("key", 0);
            rc.startGroup();
            rc.startField("x", 0);
            rc.addInteger(7);
            rc.endField("x", 0);
            rc.startField("y", 1);
            rc.addInteger(22);
            rc.endField("y", 1);
            rc.endGroup();
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.addDouble(3.14);
            rc.endField("value", 1);
            rc.endGroup();

            rc.endField("key_value", 0);
            rc.endGroup();

            rc.endField("matrix", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(record(
        record(new IntWritable(7), new IntWritable(22)),
        new DoubleWritable(3.14))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));

    deserialize(records.get(0),
        Arrays.asList("matrix"),
        Arrays.asList("map<struct<x:int,y:int>,bigint>"));
  }

  @Test
  public void testDoubleMapWithStructValue() throws Exception {
    Path test = writeDirect("DoubleMapWithStructValue",
        Types.buildMessage()
            .optionalGroup().as(MAP)
                .repeatedGroup()
                    .optional(DOUBLE).named("key")
                    .optionalGroup()
                        .required(INT32).named("x")
                        .required(INT32).named("y")
                        .named("value")
                    .named("key_value")
                .named("approx")
            .named("DoubleMapWithStructValue"),
        new TestArrayCompatibility.DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("approx", 0);

            rc.startGroup();
            rc.startField("key_value", 0);

            rc.startGroup();
            rc.startField("key", 0);
            rc.addDouble(3.14);
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.startGroup();
            rc.startField("x", 0);
            rc.addInteger(7);
            rc.endField("x", 0);
            rc.startField("y", 1);
            rc.addInteger(22);
            rc.endField("y", 1);
            rc.endGroup();
            rc.endField("value", 1);
            rc.endGroup();

            rc.endField("key_value", 0);
            rc.endGroup();

            rc.endField("approx", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(record(
        new DoubleWritable(3.14),
        record(new IntWritable(7), new IntWritable(22)))));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));

    deserialize(records.get(0),
        Arrays.asList("approx"),
        Arrays.asList("map<bigint,struct<x:int,y:int>>"));
  }

  @Test
  public void testNestedMap() throws Exception {
    Path test = writeDirect("DoubleMapWithStructValue",
        Types.buildMessage()
            .optionalGroup().as(MAP)
                .repeatedGroup()
                    .optional(BINARY).as(UTF8).named("key")
                        .optionalGroup().as(MAP)
                            .repeatedGroup()
                                .optional(BINARY).as(UTF8).named("key")
                                .required(INT32).named("value")
                            .named("key_value")
                        .named("value")
                    .named("key_value")
                .named("map_of_maps")
            .named("NestedMap"),
        new TestArrayCompatibility.DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("map_of_maps", 0);

            rc.startGroup();
            rc.startField("key_value", 0);

            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("a"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.startGroup();
            rc.startField("key_value", 0);
            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("b"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.addInteger(1);
            rc.endField("value", 1);
            rc.endGroup();
            rc.endField("key_value", 0);
            rc.endGroup();
            rc.endField("value", 1);
            rc.endGroup();

            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("b"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.startGroup();
            rc.startField("key_value", 0);
            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("a"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.addInteger(-1);
            rc.endField("value", 1);
            rc.endGroup();
            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("b"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.addInteger(-2);
            rc.endField("value", 1);
            rc.endGroup();
            rc.endField("key_value", 0);
            rc.endGroup();
            rc.endField("value", 1);
            rc.endGroup();

            rc.endField("key_value", 0);
            rc.endGroup();

            rc.endField("map_of_maps", 0);
            rc.endMessage();
          }
        });

    ArrayWritable expected = record(list(
        record(new Text("a"), list(
            record(new Text("b"), new IntWritable(1)))),
        record(new Text("b"), list(
            record(new Text("a"), new IntWritable(-1)),
            record(new Text("b"), new IntWritable(-2))))
    ));

    List<ArrayWritable> records = read(test);
    Assert.assertEquals("Should have only one record", 1, records.size());
    assertEquals("Should match expected record",
        expected, records.get(0));

    deserialize(records.get(0),
        Arrays.asList("map_of_maps"),
        Arrays.asList("map<string,map<string,int>>"));
  }
}
