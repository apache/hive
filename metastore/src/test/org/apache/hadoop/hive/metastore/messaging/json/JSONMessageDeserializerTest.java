package org.apache.hadoop.hive.metastore.messaging.json;

import org.codehaus.jackson.annotate.JsonProperty;
import org.json.JSONException;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class JSONMessageDeserializerTest {

  public static class MyClass {
    @JsonProperty
    private int a;
    @JsonProperty
    private Map<String, String> map;
    private long l;
    private String shouldNotSerialize = "shouldNotSerialize";

    //for jackson to instantiate
    MyClass() {
    }

    MyClass(int a, Map<String, String> map, long l) {
      this.a = a;
      this.map = map;
      this.l = l;
    }

    @JsonProperty
    long getL() {
      return l;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      MyClass myClass = (MyClass) o;

      if (a != myClass.a)
        return false;
      if (l != myClass.l)
        return false;
      if (!map.equals(myClass.map))
        return false;
      return shouldNotSerialize.equals(myClass.shouldNotSerialize);
    }

    @Override
    public int hashCode() {
      int result = a;
      result = 31 * result + map.hashCode();
      result = 31 * result + (int) (l ^ (l >>> 32));
      result = 31 * result + shouldNotSerialize.hashCode();
      return result;
    }
  }

  @Test
  public void shouldNotSerializePropertiesNotAnnotated() throws IOException, JSONException {
    MyClass obj = new MyClass(Integer.MAX_VALUE, new HashMap<String, String>() {{
      put("a", "a");
      put("b", "b");
    }}, Long.MAX_VALUE);
    String json = JSONMessageDeserializer.mapper.writeValueAsString(obj);
    JSONAssert.assertEquals(
        "{\"a\":2147483647,\"map\":{\"b\":\"b\",\"a\":\"a\"},\"l\":9223372036854775807}", json,
        false);
  }

  @Test
  public void shouldDeserializeJsonStringToObject() throws IOException {
    String json = "{\"a\":47,\"map\":{\"a\":\"a\",\"b\":\"a value for b\"},\"l\":98}";
    MyClass actual = JSONMessageDeserializer.mapper.readValue(json, MyClass.class);
    MyClass expected = new MyClass(47, new HashMap<String, String>() {{
      put("a", "a");
      put("b", "a value for b");
    }}, 98L);
    assertEquals(expected, actual);
  }
}