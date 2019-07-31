package net.intelie.disq.dson;

public enum DsonType {
    NULL(0x0),
    OBJECT(0x01),
    ARRAY(0x02),
    DOUBLE(0x03),
    BOOLEAN(0x04),
    STRING(0x05),
    STRING_LATIN1(0x06);

    private static final DsonType[] LOOKUP_TABLE = new DsonType[0xFF];
    private final int value;

    static {
        for (final DsonType cur : DsonType.values()) {
            LOOKUP_TABLE[cur.getValue()] = cur;
        }
    }

    DsonType(final int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static DsonType findByValue(final int value) {
        return LOOKUP_TABLE[value & 0xFF];
    }
}
