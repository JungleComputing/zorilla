package nl.vu.zorilla.util;

/**
 * Sizes of all primitive data types in java and some selected others.
 */
public final class SizeOf {
    // primitive type sizes (in bytes)
    public static final int BOOLEAN = 1; // declare a boolean to be 1 byte

    public static final int BYTE = 1;

    public static final int CHAR = 2;

    public static final int SHORT = 2;

    public static final int INT = 4;

    public static final int LONG = 8;

    public static final int FLOAT = 4;

    public static final int DOUBLE = 8;

    public static final int INET4_ADDRESS = 4;

    public static final int INET6_ADDRESS = 16;

    public static final int UUID = LONG * 2;

    //actually not fixed, but this is the assumed maximum
    public static final int BAMBOO_ID = 32;

    public static final int SHA1 = 20;
}