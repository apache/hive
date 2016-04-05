package org.apache.hadoop.hive.ql.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public final class JavaDataModelTest {

  private static final String DATA_MODEL_PROPERTY = "sun.arch.data.model";

  private String previousModelSetting;

  @Before
  public void setUp() throws Exception {
    previousModelSetting = System.getProperty(DATA_MODEL_PROPERTY);
  }

  @After
  public void tearDown() throws Exception {
    if (previousModelSetting != null) {
      System.setProperty(DATA_MODEL_PROPERTY, previousModelSetting);
    } else {
      System.clearProperty(DATA_MODEL_PROPERTY);
    }
  }

  @Test
  public void testGetDoesNotReturnNull() throws Exception {
    JavaDataModel model = JavaDataModel.get();
    assertNotNull(model);
  }

  @Test
  public void testGetModelForSystemWhenSetTo32() throws Exception {
    System.setProperty(DATA_MODEL_PROPERTY, "32");
    assertSame(JavaDataModel.JAVA32, JavaDataModel.getModelForSystem());
  }

  @Test
  public void testGetModelForSystemWhenSetTo64() throws Exception {
    System.setProperty(DATA_MODEL_PROPERTY, "64");
    assertSame(JavaDataModel.JAVA64, JavaDataModel.getModelForSystem());
  }

  @Test
  public void testGetModelForSystemWhenSetToUnknown() throws Exception {
    System.setProperty(DATA_MODEL_PROPERTY, "unknown");
    assertSame(JavaDataModel.JAVA64, JavaDataModel.getModelForSystem());
  }

  @Test
  public void testGetModelForSystemWhenUndefined() throws Exception {
    System.clearProperty(DATA_MODEL_PROPERTY);
    assertSame(JavaDataModel.JAVA64, JavaDataModel.getModelForSystem());
  }
}