package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TypeEntry stores information about a Hive Primitive TypeInfo.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class PrimitiveTypeEntry implements Writable, Cloneable {

  private static final Map<PrimitiveObjectInspector.PrimitiveCategory, PrimitiveTypeEntry>
      primitiveCategoryToTypeEntry =
      new ConcurrentHashMap<>();
  private static final Map<Class<?>, PrimitiveTypeEntry> primitiveJavaTypeToTypeEntry =
      new ConcurrentHashMap<>();
  private static final Map<Class<?>, PrimitiveTypeEntry> primitiveJavaClassToTypeEntry =
      new ConcurrentHashMap<>();
  private static final Map<Class<?>, PrimitiveTypeEntry> primitiveWritableClassToTypeEntry =
      new ConcurrentHashMap<>();

  // Base type name to PrimitiveTypeEntry map.
  private static final Map<String, PrimitiveTypeEntry> typeNameToTypeEntry =
      new ConcurrentHashMap<>();

  //find all the TypeRegistry implementations in the runtime
  private static ServiceLoader<TypeRegistry> typeRegistries =
      ServiceLoader.load(TypeRegistry.class);

  static {
    //register all the primitiveTypeEntry objects in the internal maps
    //of PrimitiveTypeEntry
    for (TypeRegistry typeRegistry : typeRegistries) {
      List<PrimitiveTypeEntry> primitiveTypeEntryList = typeRegistry.getPrimitiveTypeEntries();
      registerType(primitiveTypeEntryList);
    }
  }

  public static void registerType(List<PrimitiveTypeEntry> typeEntries) {
    for (PrimitiveTypeEntry t : typeEntries) {
      if (t.primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN) {
        primitiveCategoryToTypeEntry.put(t.primitiveCategory, t);
      }
      if (t.primitiveJavaType != null) {
        primitiveJavaTypeToTypeEntry.put(t.primitiveJavaType, t);
      }
      if (t.primitiveJavaClass != null) {
        primitiveJavaClassToTypeEntry.put(t.primitiveJavaClass, t);
      }
      if (t.primitiveWritableClass != null) {
        primitiveWritableClassToTypeEntry.put(t.primitiveWritableClass, t);
      }
      if (t.typeName != null) {
        typeNameToTypeEntry.put(t.typeName, t);
      }
    }
  }

  public static PrimitiveTypeEntry fromJavaType(Class<?> clazz) {
    return primitiveJavaTypeToTypeEntry.get(clazz);
  }

  public static PrimitiveTypeEntry fromJavaClass(Class<?> clazz) {
    return primitiveJavaClassToTypeEntry.get(clazz);
  }

  public static PrimitiveTypeEntry fromWritableClass(Class<?> clazz) {
    return primitiveWritableClassToTypeEntry.get(clazz);
  }

  public static PrimitiveTypeEntry fromPrimitiveCategory(
      PrimitiveObjectInspector.PrimitiveCategory category) {
    return primitiveCategoryToTypeEntry.get(category);
  }

  public static PrimitiveTypeEntry fromTypeName(String typeName) {
    return typeNameToTypeEntry.get(typeName);
  }
  /**
   * The category of the PrimitiveType.
   */
  public PrimitiveObjectInspector.PrimitiveCategory primitiveCategory;

  /**
   * primitiveJavaType refers to java types like int, double, etc.
   */
  public Class<?> primitiveJavaType;
  /**
   * primitiveJavaClass refers to java classes like Integer, Double, String
   * etc.
   */
  public Class<?> primitiveJavaClass;
  /**
   * writableClass refers to hadoop Writable classes like IntWritable,
   * DoubleWritable, Text etc.
   */
  public Class<?> primitiveWritableClass;
  /**
   * typeName is the name of the type as in DDL.
   */
  public String typeName;

  protected PrimitiveTypeEntry() {
    super();
  }

  public PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory primitiveCategory,
      String typeName, Class<?> primitiveType, Class<?> javaClass, Class<?> hiveClass) {
    this.primitiveCategory = primitiveCategory;
    primitiveJavaType = primitiveType;
    primitiveJavaClass = javaClass;
    primitiveWritableClass = hiveClass;
    this.typeName = typeName;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    primitiveCategory = WritableUtils.readEnum(in,
        PrimitiveObjectInspector.PrimitiveCategory.class);
    typeName = WritableUtils.readString(in);
    try {
      primitiveJavaType = Class.forName(WritableUtils.readString(in));
      primitiveJavaClass = Class.forName(WritableUtils.readString(in));
      primitiveWritableClass = Class.forName(WritableUtils.readString(in));
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {

    WritableUtils.writeEnum(out, primitiveCategory);
    WritableUtils.writeString(out, typeName);
    WritableUtils.writeString(out, primitiveJavaType.getName());
    WritableUtils.writeString(out, primitiveJavaClass.getName());
    WritableUtils.writeString(out, primitiveWritableClass.getName());
  }

  @Override
  public Object clone() {
    PrimitiveTypeEntry result = new PrimitiveTypeEntry(
        this.primitiveCategory,
        this.typeName,
        this.primitiveJavaType,
        this.primitiveJavaClass,
        this.primitiveWritableClass);
    return result;
  }

  @Override
  public String toString() {
    return typeName;
  }

}
