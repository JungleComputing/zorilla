/* $Id: ReadMessage.java 13108 2011-03-16 07:38:32Z ceriel $ */

package ibis.zorilla.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

public class Message {

    public enum MessageType {
        NORMAL, BROADCAST_HOPS, BROADCAST_LATENCY
    };

    public static final byte FALSE = 0;
    public static final byte TRUE = 1;

    private MessageType type;

    private NodeInfo source;

    private NodeInfo destination;
    
    private int moduleID;

    private int hopCount;

    // content of message
    private ByteBuffer content;

    /**
     * Maximum distance this message can travel. Can be expressed in hops, ms
     * rtt latency, or some other metric, depending on the message type.
     */
    private int maxDistance;

    public MessageType getType() {
        return type;
    }

    void setType(MessageType type) {
        this.type = type;
    }

    public NodeInfo getSource() {
        return source;
    }

    void setSource(NodeInfo source) {
        this.source = source;
    }

    public NodeInfo getDestination() {
        return destination;
    }

    void setDestination(NodeInfo destination) {
        this.destination = destination;
    }
    
    public int getModuleID() {
        return moduleID;
    }
    
    public void setModuleID(int moduleID) {
        this.moduleID = moduleID;
    }

    int getHopCount() {
        return hopCount;
    }

    void setHopCount(int hopCount) {
        this.hopCount = hopCount;
    }

    int getMaxDistance() {
        return maxDistance;
    }

    void setMaxDistance(int maxDistance) {
        this.maxDistance = maxDistance;
    }

    public ByteBuffer getContent() {
        return content;
    }

    // functions acting on the content buffer

    public int capacity() {
        return content.capacity();
    }

    public void clear() {
        content.clear();
    }

    public void flip() {
        content.flip();
    }

    public int limit() {
        return content.limit();
    }

    public void limit(int limit) {
        content.limit(limit);
    }

    public int position() {
        return content.position();
    }

    public void position(int position) {
        content.position(position);
    }

    public int remaining() {
        return content.remaining();
    }

    public void reset() {
        content.reset();
    }

    // functions for reading and writing content

    public boolean readBoolean() throws IOException {
        return content.get() == TRUE;
    }

    public byte readByte() throws IOException {
        return content.get();
    }

    public char readChar() throws IOException {
        return content.getChar();
    }

    public short readShort() throws IOException {
        return content.getShort();
    }

    public int readInt() throws IOException {
        return content.getInt();
    }

    public long readLong() throws IOException {
        return content.getLong();
    }

    public float readFloat() throws IOException {
        return content.getFloat();
    }

    public double readDouble() throws IOException {
        return content.getDouble();
    }

    public String readString() throws IOException {
        throw new IOException("cannot read strings (yet)");
    }

    public Object readObject() throws IOException, ClassNotFoundException {
        throw new IOException("cannot read objects (yet)");
    }

    public void readArray(boolean[] destination) throws IOException {
        for (int i = 0; i < destination.length; i++) {
            destination[i] = readBoolean();
        }
    }

    public void readArray(byte[] destination) throws IOException {
        content.get(destination);
    }

    public void readArray(char[] destination) throws IOException {
    }

    public void readArray(short[] destination) throws IOException {
    }

    public void readArray(int[] destination) throws IOException {
    }

    public void readArray(long[] destination) throws IOException {
    }

    public void readArray(float[] destination) throws IOException {
    }

    public void readArray(double[] destination) throws IOException {
    }

    public void readArray(Object[] destination) throws IOException,
            ClassNotFoundException {
    }

    public void readArray(boolean[] destination, int offset, int size)
            throws IOException {
    }

    public void readArray(byte[] destination, int offset, int size)
            throws IOException {
    }

    public void readArray(char[] destination, int offset, int size)
            throws IOException {
    }

    public void readArray(short[] destination, int offset, int size)
            throws IOException {
    }

    public void readArray(int[] destination, int offset, int size)
            throws IOException {
    }

    public void readArray(long[] destination, int offset, int size)
            throws IOException {
    }

    public void readArray(float[] destination, int offset, int size)
            throws IOException {
    }

    public void readArray(double[] destination, int offset, int size)
            throws IOException {
    }

    public void readArray(Object[] destination, int offset, int size)
            throws IOException, ClassNotFoundException {
    }

    public void readByteBuffer(ByteBuffer value) throws IOException,
            ReadOnlyBufferException {
    }

    public void writeBoolean(boolean value) throws IOException {
        if (value) {
            content.put(TRUE);
        } else {
            content.put(FALSE);
        }
    }

    public void writeByte(byte value) throws IOException {
        content.put(value);
    }

    public void writeChar(char value) throws IOException {
        content.putChar(value);
    }

    public void writeShort(short value) throws IOException {
        content.putShort(value);
    }

    public void writeInt(int value) throws IOException {
        content.putInt(value);
    }

    public void writeLong(long value) throws IOException {
        content.putLong(value);
    }

    public void writeFloat(float value) throws IOException {
        content.putFloat(value);
    }

    public void writeDouble(double value) throws IOException {
        content.putDouble(value);
    }

    public void writeString(String value) throws IOException {
        throw new IOException("cannot write strings (yet)");
    }

    public void writeObject(Object value) throws IOException {
        throw new IOException("cannot write objects (yet)");
    }

    public void writeArray(boolean[] value) throws IOException {
        for (int i = 0; i < value.length; i++) {
            writeBoolean(value[i]);
        }
    }

    public void writeArray(byte[] value) throws IOException {
        content.put(value);
    }

    public void writeArray(char[] value) throws IOException {
        content.asCharBuffer().put(value);
    }

    public void writeArray(short[] value) throws IOException {
        content.asShortBuffer().put(value);
    }

    public void writeArray(int[] value) throws IOException {
        content.asIntBuffer().put(value);
    }

    public void writeArray(long[] value) throws IOException {
        content.asLongBuffer().put(value);
    }

    public void writeArray(float[] value) throws IOException {
        content.asFloatBuffer().put(value);
    }

    public void writeArray(double[] value) throws IOException {
        content.asDoubleBuffer().put(value);
    }

    public void writeArray(Object[] value) throws IOException {
        for (int i = 0; i < value.length; i++) {
            writeObject(value);
        }
    }

    public void writeArray(boolean[] value, int offset, int length)
            throws IOException {
        for (int i = offset; i < offset + length; i++) {
            writeBoolean(value[i]);
        }
    }

    public void writeArray(byte[] value, int offset, int length)
            throws IOException {
        content.put(value, offset, length);
    }

    public void writeArray(char[] value, int offset, int length)
            throws IOException {
        content.asCharBuffer().put(value, offset, length);
    }

    public void writeArray(short[] value, int offset, int length)
            throws IOException {
        content.asShortBuffer().put(value, offset, length);
    }

    public void writeArray(int[] value, int offset, int length)
            throws IOException {
        content.asIntBuffer().put(value, offset, length);
    }

    public void writeArray(long[] value, int offset, int length)
            throws IOException {
        content.asLongBuffer().put(value, offset, length);
    }

    public void writeArray(float[] value, int offset, int length)
            throws IOException {
        content.asFloatBuffer().put(value, offset, length);
    }

    public void writeArray(double[] value, int offset, int length)
            throws IOException {
        content.asDoubleBuffer().put(value, offset, length);
    }

    public void writeArray(Object[] value, int offset, int length)
            throws IOException {
        for (int i = offset; i < offset + length; i++) {
            writeObject(value[i]);
        }
    }

    public void writeByteBuffer(ByteBuffer value) throws IOException {
        content.put(value);
    }
}
