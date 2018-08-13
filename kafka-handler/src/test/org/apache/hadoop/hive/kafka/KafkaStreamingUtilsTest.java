//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.hadoop.hive.kafka;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class KafkaStreamingUtilsTest {
  public KafkaStreamingUtilsTest() {
  }

  @Test
  public void testConsumerProperties() {
    Configuration configuration = new Configuration();
    configuration.set("kafka.bootstrap.servers", "localhost:9090");
    configuration.set("kafka.consumer.fetch.max.wait.ms", "40");
    configuration.set("kafka.consumer.my.new.wait.ms", "400");
    Properties properties = KafkaStreamingUtils.consumerProperties(configuration);
    Assert.assertEquals("localhost:9090", properties.getProperty("bootstrap.servers"));
    Assert.assertEquals("40", properties.getProperty("fetch.max.wait.ms"));
    Assert.assertEquals("400", properties.getProperty("my.new.wait.ms"));
  }
}
