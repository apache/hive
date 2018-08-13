//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.hadoop.hive.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class KafkaRecordWritableTest {
  public KafkaRecordWritableTest() {
  }

  @Test
  public void testWriteReadFields() throws IOException {
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord("topic", 0, 3L, "key".getBytes(), "value".getBytes());
    KafkaRecordWritable kafkaRecordWritable = KafkaRecordWritable.fromKafkaRecord(record);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream w = new DataOutputStream(baos);
    kafkaRecordWritable.write(w);
    w.flush();

    ByteArrayInputStream input = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream inputStream = new DataInputStream(input);
    KafkaRecordWritable actualKafkaRecordWritable = new KafkaRecordWritable();
    actualKafkaRecordWritable.readFields(inputStream);
    Assert.assertEquals(kafkaRecordWritable, actualKafkaRecordWritable);
  }
}
