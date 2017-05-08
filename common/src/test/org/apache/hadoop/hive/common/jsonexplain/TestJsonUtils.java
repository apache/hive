package org.apache.hadoop.hive.common.jsonexplain;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by dvoros on 1/12/17.
 */
public class TestJsonUtils {

  private static final String TEST_KEY = "key";
  private static final String TEST_VALUE_ONE = "one";
  private static final String TEST_VALUE_TWO = "two";
  private static final JSONArray TEST_ARRAY;

  static {
    TEST_ARRAY = new JSONArray();
    TEST_ARRAY.add(TEST_VALUE_ONE);
  }

  @Test
  public void testAccumulateNewKeyWithSimpleObject() throws Exception {
    JSONObject obj = new JSONObject();
    JsonUtils.accumulate(obj, TEST_KEY, TEST_VALUE_ONE);
    assertEquals(obj.get(TEST_KEY), TEST_VALUE_ONE);
  }

  @Test
  public void testAccumulateNewKeyWithArray() throws Exception {
    JSONObject obj = new JSONObject();
    JsonUtils.accumulate(obj, TEST_KEY, TEST_ARRAY);
    assertEquals(((JSONArray)obj.get(TEST_KEY)).size(), 1);
    assertEquals(((JSONArray)obj.get(TEST_KEY)).get(0), TEST_ARRAY);
  }

  @Test
  public void testAccumulateAlreadyPresentArrayNonEmpty() throws Exception {
    JSONObject obj = new JSONObject();
    JSONArray arr = new JSONArray();
    arr.add(TEST_VALUE_ONE);
    obj.put(TEST_KEY, arr);
    JsonUtils.accumulate(obj, TEST_KEY, TEST_VALUE_TWO);
    assertEquals(((JSONArray)obj.get(TEST_KEY)).size(), 2);
    assertEquals(((JSONArray)obj.get(TEST_KEY)).get(0), TEST_VALUE_ONE);
    assertEquals(((JSONArray)obj.get(TEST_KEY)).get(1), TEST_VALUE_TWO);
  }

  @Test
  public void testAccumulateAlreadyPresentArrayEmpty() throws Exception {
    JSONObject obj = new JSONObject();
    obj.put(TEST_KEY, new JSONArray());
    JsonUtils.accumulate(obj, TEST_KEY, TEST_VALUE_TWO);
    assertEquals(((JSONArray)obj.get(TEST_KEY)).size(), 1);
    assertEquals(((JSONArray)obj.get(TEST_KEY)).get(0), TEST_VALUE_TWO);
  }

  @Test
  public void testAccumulateAlreadyPresentNonArray() throws Exception {
    JSONObject obj = new JSONObject();
    obj.put(TEST_KEY, TEST_VALUE_ONE);
    JsonUtils.accumulate(obj, TEST_KEY, TEST_VALUE_TWO);
    assertEquals(((JSONArray)obj.get(TEST_KEY)).size(), 2);
    assertEquals(((JSONArray)obj.get(TEST_KEY)).get(0), TEST_VALUE_ONE);
    assertEquals(((JSONArray)obj.get(TEST_KEY)).get(1), TEST_VALUE_TWO);
  }

  @Test
  public void testGetNamesEmpty() throws Exception {
    JSONObject obj = new JSONObject();
    String[] result = JsonUtils.getNames(obj);
    assertEquals(0, result.length);
  }

  @Test
  public void testGetNamesSingle() throws Exception {
    JSONObject obj = new JSONObject();
    obj.put("key1", "value1");
    String[] result = JsonUtils.getNames(obj);
    assertEquals(1, result.length);
    assertEquals("key1", result[0]);
  }

  @Test
  public void testGetNamesMultiple() throws Exception {
    JSONObject obj = new JSONObject();
    obj.put("key1", "value1");
    obj.put("key2", "value2");
    obj.put("key3", "value3");
    List<String> result = Arrays.asList(JsonUtils.getNames(obj));
    assertEquals(3, result.size());
    assertTrue(result.contains("key1"));
    assertTrue(result.contains("key2"));
    assertTrue(result.contains("key3"));
  }

}
