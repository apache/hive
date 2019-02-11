package org.apache.hadoop.hive.kafka;

import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 * Test class for Hive Kafka Avro bytes converter.
 */
public class AvroBytesConverterTest {

    private static SimpleRecord simpleRecord1 = SimpleRecord.newBuilder().setId("123").setName("test").build();
    private static byte[] simpleRecord1AsBytes;

    /**
     * Emulate confluent avro producer that add 4 magic bits (int) before value bytes. The int represents the schema ID from schema registry.
     */
    @BeforeClass
    public static void setUp() {
        Map<String,String> config = Maps.newHashMap();
        config.put("schema.registry.url","http://localhost");
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(new MockSchemaRegistryClient());
        avroSerializer.configure(config, false);
        simpleRecord1AsBytes = avroSerializer.serialize("temp", simpleRecord1);
    }

    /**
     * Emulate - avro.serde.type = none (Default)
     */
    @Test
    public void convertWithAvroBytesConverter() {
        Schema schema = SimpleRecord.getClassSchema();
        KafkaSerDe.AvroBytesConverter conv = new KafkaSerDe.AvroBytesConverter(schema);
        AvroGenericRecordWritable simpleRecord1Writable = conv.getWritable(simpleRecord1AsBytes);

        Assert.assertNotNull(simpleRecord1Writable);
        Assert.assertEquals(SimpleRecord.class,simpleRecord1Writable.getRecord().getClass());

        SimpleRecord simpleRecord1Deserialized = (SimpleRecord) simpleRecord1Writable.getRecord();

        Assert.assertNotNull(simpleRecord1Deserialized);
        Assert.assertNotEquals(simpleRecord1, simpleRecord1Deserialized);
    }


    /**
     * Emulate - avro.serde.type = confluent
     */
    @Test
    public void convertWithConfluentAvroBytesConverter() {
        Schema schema = SimpleRecord.getClassSchema();
        KafkaSerDe.AvroSkipBytesConverter conv = new KafkaSerDe.AvroSkipBytesConverter(schema, 5);
        AvroGenericRecordWritable simpleRecord1Writable = conv.getWritable(simpleRecord1AsBytes);

        Assert.assertNotNull(simpleRecord1Writable);
        Assert.assertEquals(SimpleRecord.class,simpleRecord1Writable.getRecord().getClass());

        SimpleRecord simpleRecord1Deserialized = (SimpleRecord) simpleRecord1Writable.getRecord();

        Assert.assertNotNull(simpleRecord1Deserialized);
        Assert.assertEquals(simpleRecord1, simpleRecord1Deserialized);
    }
}
