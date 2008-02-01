package nl.vu.zorilla.bigNet.bamboo;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Formatter;

import nl.vu.zorilla.util.SizeOf;

/**
 * immutable location in the p2p network used in zorilla
 */
final class Location {

    public static final int COORDINATE_DIMENSIONS = 5;

    public static int SIZE = SizeOf.BYTE + SizeOf.BAMBOO_ID
            + (SizeOf.DOUBLE * COORDINATE_DIMENSIONS); // bytes

    private final BigInteger bambooID;

    private final double[] coordinate;

    public static Location nowhere;

    static {
        nowhere = new Location();
    }

    // TODO: change this to a ZorillaP2P network coordinate

    Location() {
        bambooID = BigInteger.ZERO;
        coordinate = new double[COORDINATE_DIMENSIONS];
    }

    Location(BigInteger bambooID, double[] coordinate) {
        this.bambooID = bambooID;

        assert coordinate.length == COORDINATE_DIMENSIONS;
        this.coordinate = coordinate.clone();

    }

    Location(BigInteger bambooID) {
        this.bambooID = bambooID;
        this.coordinate = new double[COORDINATE_DIMENSIONS];
    }

    private byte[] bambooIdBytes() {
        byte[] result = new byte[SizeOf.BYTE + SizeOf.BAMBOO_ID];

        byte[] id = bambooID.toByteArray();

        result[0] = (byte) id.length;

        for (int i = 0; i < id.length; i++) {
            result[i + 1] = id[i];
        }

        return result;
    }

    Location(DataInput in) throws IOException {
        byte[] data = new byte[SizeOf.BYTE + SizeOf.BAMBOO_ID];

        in.readArray(data);

        byte[] bambooIdBytes = new byte[data[0]];

        for (int i = 0; i < bambooIdBytes.length; i++) {
            bambooIdBytes[i] = data[i + 1];
        }

        bambooID = new BigInteger(bambooIdBytes);

        coordinate = new double[COORDINATE_DIMENSIONS];
        in.readArray(coordinate);

    }

    Location(ByteBuffer buffer) {
        byte[] data = new byte[SizeOf.BYTE + SizeOf.BAMBOO_ID];

        buffer.get(data);

        byte[] bambooIdBytes = new byte[data[0]];

        for (int i = 0; i < bambooIdBytes.length; i++) {
            bambooIdBytes[i] = data[i + 1];
        }

        bambooID = new BigInteger(bambooIdBytes);

        coordinate = new double[COORDINATE_DIMENSIONS];

        for (int i = 0; i < COORDINATE_DIMENSIONS; i++) {
            coordinate[i] = buffer.getDouble();
        }

    }

    boolean isNowhere() {
        return bambooID.equals(nowhere);
    }

    boolean origin() {
        for (int i = 0; i < COORDINATE_DIMENSIONS; i++) {
            if (coordinate[i] != 0) {
                return false;
            }
        }
        return true;
    }

    boolean complete() {
        return !bambooID.equals(BigInteger.ZERO);
    }

    void writeTo(DataOutput out) throws IOException {
        out.writeArray(bambooIdBytes());
        out.writeArray(coordinate);

    }

    void writeTo(ByteBuffer buffer) {
        buffer.put(bambooIdBytes());
        for (int i = 0; i < COORDINATE_DIMENSIONS; i++) {
            buffer.putDouble(coordinate[i]);
        }
    }

    BigInteger bambooID() {
        return bambooID;
    }

    private String double2String(double value) {
        StringBuilder stringBuilder = new StringBuilder();
        Formatter formatter = new Formatter(stringBuilder);

        formatter.format("%.2f", value);

        return stringBuilder.toString();
    }

    public String toString() {
        String result = bambooID.toString(16);
        while (result.length() < 40) {
            result = "0" + result;
        }
        result = result.substring(0, 8);

        if (!origin()) {
            result += " @ ";

            for (int i = 0; i < COORDINATE_DIMENSIONS - 1; i++) {
                result += double2String(coordinate[i]) + ", ";

            }
            result += double2String(coordinate[COORDINATE_DIMENSIONS - 1]);
        }
        return result;
    }

    public double distanceTo(Location other) {
        double result = 0;

        assert (coordinate.length == other.coordinate.length);

        for (int i = 0; i < coordinate.length; i++) {
            result += (coordinate[i] - other.coordinate[i])
                    * (coordinate[i] - other.coordinate[i]);
        }

        result = Math.sqrt(result);

        return result;
    }
   }
