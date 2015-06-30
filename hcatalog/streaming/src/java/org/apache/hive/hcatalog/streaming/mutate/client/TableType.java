package org.apache.hive.hcatalog.streaming.mutate.client;

public enum TableType {
  SOURCE((byte) 0),
  SINK((byte) 1);

  private static final TableType[] INDEX = buildIndex();

  private static TableType[] buildIndex() {
    TableType[] index = new TableType[TableType.values().length];
    for (TableType type : values()) {
      byte position = type.getId();
      if (index[position] != null) {
        throw new IllegalStateException("Overloaded index: " + position);
      }
      index[position] = type;
    }
    return index;
  }

  private byte id;

  private TableType(byte id) {
    this.id = id;
  }

  public byte getId() {
    return id;
  }

  public static TableType valueOf(byte id) {
    if (id < 0 || id >= INDEX.length) {
      throw new IllegalArgumentException("Invalid id: " + id);
    }
    return INDEX[id];
  }
}
