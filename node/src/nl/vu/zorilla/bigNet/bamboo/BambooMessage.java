package nl.vu.zorilla.bigNet.bamboo;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URISyntaxException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;
import org.gridlab.gat.URI;

import nl.vu.zorilla.ZorillaError;
import nl.vu.zorilla.ZorillaException;
import nl.vu.zorilla.bigNet.Function;
import nl.vu.zorilla.bigNet.Message;
import nl.vu.zorilla.io.ObjectInput;
import nl.vu.zorilla.io.ObjectOutput;
import nl.vu.zorilla.util.SizeOf;

/**
 * Object representing a message in the p2p system. A message is always send as
 * a single packet. A message is made up of a fixed size header followed by a
 * variable length payload. The header is make up of the following data, al in
 * network (big endian) byte order
 * 
 * <BR>- the total length of the message <BR>- the identifier of the sender
 * node <BR>- the identifier of the receiver node <BR>- the identifier of the
 * sending object <BR>- the identifier of the receiving object <BR>- the
 * unique id of this message <BR>- the module this message was send to and from
 * <BR>- the type of the message <BR>- the byteorder of the payload <BR>- the
 * time-to-live(TTL) of the message
 * 
 * the header is followed by a variable length payload.
 * 
 */
public final class BambooMessage implements DataOutput, DataInput, Message {

    private static final int MESSAGE_LENGTH_POSITION = 0;

    private static final int SOURCE_POSITION = MESSAGE_LENGTH_POSITION
            + SizeOf.INT;

    private static final int DESTINATION_POSITION = SOURCE_POSITION
            + Address.SIZE;

    private static final int MESSAGE_ID_POSITION = DESTINATION_POSITION
            + Address.SIZE;

    private static final int JOB_ID_POSITION = MESSAGE_ID_POSITION
            + SizeOf.UUID;

    private static final int MESSAGE_FUNCTION_POSITION = JOB_ID_POSITION
            + SizeOf.UUID;
    
    private static final int MESSAGE_TYPE_POSITION = MESSAGE_FUNCTION_POSITION + SizeOf.INT;

    private static final int FLAGS_POSITION = MESSAGE_TYPE_POSITION + SizeOf.INT;
    
    // masks

    private static final byte BIG_ENDIAN_MASK = 0x01; // 00000001

    private static final int TTL_POSITION = FLAGS_POSITION + SizeOf.BYTE;

    private static final int HEADER_SIZE = TTL_POSITION + SizeOf.INT;

    private static final int MAX_MESSAGE_SIZE = 55 * 1024;
//private static final int MAX_MESSAGE_SIZE = 63 * 1024;

    public static final int MAX_PAYLOAD_SIZE = MAX_MESSAGE_SIZE - HEADER_SIZE;

    private static final byte TRUE = 1;

    private static final byte FALSE = 0;

    // TODO: benchmark message cache

    private static final int MESSAGE_CACHE_SIZE = 128;

    private static Logger logger = Logger.getLogger(BambooMessage.class);

    private static BambooMessage[] cache = new BambooMessage[MESSAGE_CACHE_SIZE];

    private static int cacheSize = 0;

    private static final byte[] emptyHeader = new byte[HEADER_SIZE];

    /**
     * Retrieves a empty message from the cache.
     * 
     * @return an empty message
     */
    public synchronized static BambooMessage getMessage() {
        if (cacheSize > 0) {
            BambooMessage result;
            cacheSize -= 1;
            result = cache[cacheSize];
            result.clearMessage();
            return result;
        }

        return new BambooMessage();
    }

    /**
     * Returns a message to the message cache for recycling. Will throw away the
     * message if the cache is full.
     * 
     * @param message
     *            the message to be recycled.
     */
    public synchronized static void recycle(BambooMessage message) {
        if (message.clone) {
            // don't recylce clones.
            return;
        }

        if (cacheSize < MESSAGE_CACHE_SIZE) {
            cache[cacheSize] = message;
            cacheSize++;
        }
    }

    /**
     * create a new message from an array of bytes
     */
    public static BambooMessage fromBytes(byte[] messageBytes) throws IOException {
        BambooMessage m = getMessage();

        m.buffer.clear();
        m.buffer.put(messageBytes);

        if (m.buffer.position() < HEADER_SIZE
                || m.buffer.position() != m.getHeaderMessageLength()) {
            throw new ZorillaError("received incomplete message");
        }

        m.readHeader();

        return m;
    }

    private MessageType type = null;
    
    private Function function = null;

    // bytebuffer the message is constructed in.
    private ByteBuffer buffer;

    // payload part of the message, in the byte order specified by
    // payloadByteOrder
    private ByteBuffer payload;

    // cache of header fields

    private Address source = null;

    private Address destination = null;

    private UUID id = null;

    private UUID jobID = null;

    private SocketAddress receivedFrom = null;

    // keep track of partial sends and receives

    private boolean sending = false;

    private boolean receiving = false;

    private boolean clone = false;

    /**
     * create a new empty message, used for writing data to.
     */
    private BambooMessage() {
        // create a BIG_ENDIAN buffer
        buffer = ByteBuffer.allocateDirect(MAX_MESSAGE_SIZE);

        buffer.position(HEADER_SIZE);

        // set the payload as a partial view of the buffer
        payload = buffer.slice();
        payload.order(ByteOrder.nativeOrder());

        buffer.clear();
        payload.clear();
    }

    /**
     * copy constuctor. does not copy contents, thus shares data with other
     * messages
     * 
     * @param original
     *            the message to copy
     */
    private BambooMessage(BambooMessage original, boolean copy) {

        if (!copy) {
            buffer = original.buffer.duplicate();
            payload = original.payload.duplicate();

            // copy byte order
            payload.order(original.payload.order());

            clone = true;
        } else {
            buffer = ByteBuffer.allocateDirect(MAX_MESSAGE_SIZE);
            buffer.position(HEADER_SIZE);

            // set the payload as a partial view of the buffer
            payload = buffer.slice();
            payload.order(original.payload.order());
            payload.position(original.payload.position());
            payload.limit(original.payload.limit());

            int position = original.buffer.position();
            int limit = original.buffer.limit();

            // copy over contents of ENTIRE buffer
            buffer.position(0).limit(buffer.capacity());
            original.buffer.position(0).limit(buffer.capacity());

            while (buffer.hasRemaining()) {
                buffer.put(original.buffer);
            }

            buffer.position(position);
            buffer.limit(limit);
            original.buffer.position(position);
            original.buffer.limit(limit);

            clone = false;
        }

        type = original.type;
        function = original.function;
        receivedFrom = original.receivedFrom;
        source = original.source;
        destination = original.destination;
        id = original.id;
        jobID = original.jobID;
        sending = original.sending;
        receiving = original.receiving;
    }

    BambooMessage duplicate() {
        return new BambooMessage(this, false);
    }

    public BambooMessage copy() {
        if (logger.isDebugEnabled()) {
            logger.debug("copying : " + toVerboseString());
        }
        BambooMessage result = new BambooMessage(this, true);
        if (logger.isDebugEnabled()) {
            logger.debug("result : " + result.toVerboseString());
        }

        return result;
    }

    public void clearMessage() {
        buffer.clear();
        buffer.put(emptyHeader);

        payload.order(ByteOrder.nativeOrder());

        buffer.clear();
        payload.clear();

        source = null;
        destination = null;
        id = null;
        jobID = null;
        type = null;
        function = null;

        sending = false;
        receiving = false;

    }

    private int getHeaderMessageLength() throws IOException {
        int result = buffer.getInt(MESSAGE_LENGTH_POSITION);
        if (result > MAX_MESSAGE_SIZE) {
            throw new IOException("illegal message size in message header");
        }
        return result;
    }

    private void setHeaderMessageLengt(int length) {
        if (length > MAX_MESSAGE_SIZE) {
            throw new ZorillaError("tried to set message length to"
                    + " above maximum");
        }
        buffer.putInt(MESSAGE_LENGTH_POSITION, length);
    }

    // payloadlength, as currently in header
    private int getHeaderPayloadLength() throws IOException {
        int result = buffer.getInt(MESSAGE_LENGTH_POSITION) - HEADER_SIZE;
        if (result > MAX_PAYLOAD_SIZE) {
            throw new IOException("illegal payload size in message header");
        }
        return result;
    }

    MessageType getType() {
        return type;
    }

    void setType(MessageType type) {
        this.type = type;
    }
    
    public Function getFunction() {
        return function;
    }
    
    public void setFunction(Function function) {
        this.function = function;
    }

    public Address getSource() {
        return source;
    }

    void setSource(Address source) {
        this.source = source;
    }

    public Address getDestination() {
        return destination;
    }

    void setDestination(Address destination) {
        this.destination = destination;
    }

    void setID(UUID id) {
        this.id = id;
    }

    UUID getId() {
        return id;
    }

    public UUID getJobID() {
        return jobID;
    }

    void setJobID(UUID jobID) {
        this.jobID = jobID;
    }

    int getTtl() {
        return buffer.getInt(TTL_POSITION);
    }

    void setTtl(int ttl) {
        buffer.putInt(TTL_POSITION, ttl);
    }

    private void writeHeader() {
        assert source != null : "message source not filled in";
        assert destination != null : "message destination not filled in";
        assert id != null : "message id not filled in";
        assert type != null : "message type not filled in";
        assert function != null : "message function not filled in";

        logger.debug("writing :\n" + toVerboseString());

        if (clone) {
            return;
        }
        int dataSize = payload.limit() + HEADER_SIZE;

        buffer.position(SOURCE_POSITION);

        source.writeTo(buffer);

        buffer.position(DESTINATION_POSITION);

        destination.writeTo(buffer);

        buffer.position(MESSAGE_ID_POSITION);
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        
        buffer.position(MESSAGE_FUNCTION_POSITION);
        buffer.putInt(function.ordinal());

        buffer.position(MESSAGE_TYPE_POSITION);
        buffer.putInt(type.ordinal());
        
        buffer.position(JOB_ID_POSITION);
        if (jobID == null) {
            buffer.putLong(0L);
            buffer.putLong(0L);
        } else {
            buffer.putLong(jobID.getMostSignificantBits());
            buffer.putLong(jobID.getLeastSignificantBits());
        }

        // add message length, and payload byte order
        setHeaderMessageLengt(dataSize);

        // lame by-hand merge of al flags to one byte value

        byte flags = 0;

        if (payload.order() == ByteOrder.BIG_ENDIAN) {
            flags |= BIG_ENDIAN_MASK;
        }

        logger.debug("flags = " + flags);

        buffer.put(FLAGS_POSITION, flags);

        // set up buffer so it can be drained
        buffer.position(0);
        buffer.limit(dataSize);
        
        logger.debug("total message size: " + dataSize);
    }

    /**
     * Writes a message to a socket channel. NOTE: this function is called by
     * TcpHandler, and should not be used anywhere else.
     * 
     * @param channel
     *            The channel to write the message to.
     * 
     * @return true if the message has been completely send, false if not.
     */
    boolean sendMessage(SocketChannel channel) throws IOException {
        if (!sending) {
            writeHeader();

            sending = true;
        }

        channel.write(buffer);

        if (buffer.remaining() == 0) {
            // message send completely;
            sending = false;
            return true;
        }
        return false;
    }

    /**
     * Sends a message as a datagram through the specified channel. This
     * function is called by the UdpHandler, and should not be used elsewhere.
     * 
     * @param channel
     *            the channel to send the message through
     */
    void sendMessage(DatagramChannel channel, InetSocketAddress socketAddress)
            throws IOException {
        writeHeader();
        buffer.mark();

        channel.send(buffer, socketAddress);

        if (buffer.hasRemaining()) {
            throw new IOException("could not send message");
        }
        buffer.reset();
    }

    private void readHeader() throws IOException {

        buffer.clear();

        byte flags = buffer.get(FLAGS_POSITION);

        logger.debug("flags = " + flags);
        
        if ((flags & BIG_ENDIAN_MASK) == 0) {
            payload.order(ByteOrder.LITTLE_ENDIAN);
        } else {
            payload.order(ByteOrder.BIG_ENDIAN);
        }

        buffer.position(MESSAGE_TYPE_POSITION);
        int typeValue = buffer.getInt();

        try {
            type = MessageType.fromOrdinal(typeValue);
        } catch (ZorillaException e) {
            throw new IOException(e.toString());
        }
        
        logger.debug("received message, type = " + this.type);
        
        buffer.position(MESSAGE_FUNCTION_POSITION);
        int functionValue = buffer.getInt();
        
        try {
            function = Function.fromOrdinal(functionValue);
        } catch (ZorillaException e) {
            throw new IOException(e.toString());
        }

        payload.position(0).limit(getHeaderPayloadLength());

        buffer.position(SOURCE_POSITION);
        source = new Address(buffer);

        buffer.position(DESTINATION_POSITION);
        destination = new Address(buffer);

        buffer.position(MESSAGE_ID_POSITION);
        long idMsb = buffer.getLong();
        long idLsb = buffer.getLong();
        
        if (idMsb == 0L && idLsb == 0L) {
            throw new IOException("message id 0!");
        }
        id = new UUID(idMsb, idLsb);

        buffer.position(JOB_ID_POSITION);
        long jobMsb = buffer.getLong();
        long jobLsb = buffer.getLong();
        
        if (jobMsb == 0L && jobLsb == 0L) {
            jobID = null;
        } else {
            jobID = new UUID(jobMsb, jobLsb);
        }

        buffer.clear();

        logger.debug("read :\n" + toVerboseString());
    }

    /**
     * Receive a message from a socket channel.
     * 
     * @param channel
     *            the channel to receive the message from
     * @return if the message was completely received, or only partial.
     * 
     * @throws IOException
     *             in case of trouble
     */
    boolean receiveMessage(SocketChannel channel) throws IOException {
        int bytesRead;

        if (!receiving) {
            buffer.clear();
            receiving = true;
        }

        if (buffer.position() < SizeOf.INT) {
            // total length of message still unknown, try to receive it

            buffer.limit(HEADER_SIZE);

            int read = channel.read(buffer);

            if (read == -1) {
                throw new ClosedChannelException();
            }

            if (buffer.position() < SizeOf.INT) {
                // message length still unknown... exit
                return false;
            }
        }

        buffer.limit(getHeaderMessageLength());

        // do a read
        bytesRead = channel.read(buffer);

        if (bytesRead == -1) {
            throw new IOException("end of stream encountered");
        }

        if (buffer.remaining() == 0) {
            // message completely received

            readHeader();

            receivedFrom = channel.socket().getRemoteSocketAddress();
            receiving = false;
            return true;
        }
        return false;
    }

    /**
     * Receives a message via a datagram channel. Each datagram should contain
     * exactly one message. This function is called by the UdpHandler, and
     * should not be used elsewhere
     * 
     * @param channel
     *            the datagram channel to fetch the data from.
     * 
     * @throws IOException
     *             in case of trouble.
     */
    boolean receiveMessage(DatagramChannel channel) throws IOException {
        buffer.clear();

        receivedFrom = channel.receive(buffer);

        if (receivedFrom == null) {
            logger.debug("receive got us nothing");
            return false;
        }

        if (buffer.position() < HEADER_SIZE
                || buffer.position() < getHeaderMessageLength()) {
            throw new IOException("received incomplete message");
        }

        readHeader();

        return true;
    }

    public int getPayloadPosition() {
        return payload.position();
    }

    public void setPayloadPosition(int position) {
        payload.position(position);
    }

    void flip() {
        payload.flip();
    }

    /**
     * returns the limit of the payload buffer.
     * 
     * @return the limit of the payload.
     */
    public int getPayloadLimit() {
        return payload.limit();
    }

    /**
     * Sets the limit of the payload
     * 
     * @param limit
     *            the new limit of the payload buffer
     */
    public void setPayloadLimit(int limit) {
        payload.limit(limit);
    }

    public int payloadRemaining() {
        return payload.remaining();
    }

    public boolean hasRemaining() {
        return payload.hasRemaining();
    }

    public void clearPayload() {
        payload.clear();
    }

    // *** IbisDataOutput and Input interface implementation.

    public void writeBoolean(boolean value) throws IOException {
        try {
            if (value) {
                payload.put(TRUE);
            } else {
                payload.put(FALSE);
            }
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeByte(byte value) throws IOException {
        try {
            payload.put(value);
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeChar(char value) throws IOException {
        try {
            payload.putChar(value);
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeShort(short value) throws IOException {
        try {
            payload.putShort(value);
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeInt(int value) throws IOException {
        try {
            payload.putInt(value);
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeLong(long value) throws IOException {
        try {
            payload.putLong(value);
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeFloat(float value) throws IOException {
        try {
            payload.putFloat(value);
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeDouble(double value) throws IOException {
        try {
            payload.putDouble(value);
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeArray(boolean[] src, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                writeBoolean(src[i]);
            }
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeArray(byte[] src, int offset, int length)
            throws IOException {
        try {
            payload.put(src, offset, length);
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeArray(char[] src, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                writeChar(src[i]);
            }
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeArray(short[] src, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                writeShort(src[i]);
            }
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeArray(int[] src, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                writeInt(src[i]);
            }
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeArray(long[] src, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                writeLong(src[i]);
            }
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeArray(float[] src, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                writeFloat(src[i]);
            }
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public void writeArray(double[] src, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                writeDouble(src[i]);
            }
        } catch (BufferOverflowException e) {
            throw new IOException("message full");
        }
    }

    public boolean readBoolean() throws IOException {
        try {
            byte value = payload.get();

            return (value == TRUE);
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public byte readByte() throws IOException {
        try {
            return payload.get();
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public char readChar() throws IOException {
        try {
            return payload.getChar();
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public short readShort() throws IOException {
        try {
            return payload.getShort();
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public int readInt() throws IOException {
        try {
            return payload.getInt();
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public long readLong() throws IOException {
        try {
            return payload.getLong();
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public float readFloat() throws IOException {
        try {
            return payload.getFloat();
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public double readDouble() throws IOException {
        try {
            return payload.getDouble();
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public void readArray(boolean[] dst, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                dst[i] = readBoolean();
            }
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public void readArray(byte[] dst, int offset, int length)
            throws IOException {
        try {
            payload.get(dst, offset, length);
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public void readArray(char[] dst, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                dst[i] = readChar();
            }
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public void readArray(short[] dst, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                dst[i] = readShort();
            }
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public void readArray(int[] dst, int offset, int length) throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                dst[i] = readInt();
            }
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public void readArray(long[] dst, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                dst[i] = readLong();
            }
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public void readArray(float[] dst, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                dst[i] = readFloat();
            }
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public void readArray(double[] dst, int offset, int length)
            throws IOException {
        try {
            for (int i = offset; i < (offset + length); i++) {
                dst[i] = readDouble();
            }
        } catch (BufferUnderflowException e) {
            throw new IOException("no more data left in message");
        }
    }

    public String toVerboseString() {
        Address source = getSource();
        if (source == null) {
            source = Address.UNKNOWN;
        }

        Address destination = getDestination();
        if (destination == null) {
            destination = Address.UNKNOWN;
        }
        
        int length;
        try {
            length = getHeaderMessageLength();
        } catch (IOException e) {
            //IGNORE
            length = -1;
        }

        return "\n*** message #" + getId() + " ***" + "\n Source:             "
                + source.toVerboseString() + "\n Destination:        "
                + destination.toVerboseString() + "\n Type:               "
                + getType() + "\n Job ID:             " + getJobID()
                + "\n received from:      " + receivedFrom()
                + "\n ttl:                " + getTtl() + "\n function:           "
                + getFunction() + "\n payload pos/limit:  "
                + payload.position() + "/" + payload.limit() + " bytes"
                + "\n message length:     " + length
                + "\n payload byte order: " + payload.order() + "\n***";
    }

    public SocketAddress receivedFrom() {
        return receivedFrom;
    }

    public String toString() {
        return "MESSAGE:" + getId() + " from " + getSource();
    }

    public void writeString(String string) throws IOException {
        byte[] bytes = string.getBytes("UTF-8");
        if ((bytes.length + SizeOf.INT) > payloadRemaining()) {
            throw new IOException("message full");
        }
        writeInt(bytes.length);
        writeArray(bytes, 0, bytes.length);
    }

    public void writeStrings(String[] strings) throws IOException {
        writeInt(strings.length);

        for (int i = 0; i < strings.length; i++) {
            writeString(strings[i]);
        }
    }

    public void writeStringMap(Map<String, String> stringMap)
            throws IOException {
        Object[] entries = stringMap.entrySet().toArray();

        writeInt(entries.length);

        for (int i = 0; i < entries.length; i++) {
            Map.Entry entry = (Map.Entry) entries[i];
            writeString((String) entry.getKey());
            writeString((String) entry.getValue());
        }
    }

    public void writeURI(URI uri) throws IOException {
        writeString(uri.toString());
    }

    public void writeURIs(URI[] uris) throws IOException {
        writeInt(uris.length);

        for (int i = 0; i < uris.length; i++) {
            writeURI(uris[i]);
        }
    }

    public void writeUUID(UUID uuid) {
        if (uuid == null) {
            payload.putLong(0L);
            payload.putLong(0L);
        } else {
            payload.putLong(uuid.getMostSignificantBits());
            payload.putLong(uuid.getLeastSignificantBits());
        }
    }

    public void writeUUIDs(UUID[] uuids) throws IOException {
        writeInt(uuids.length);

        for (int i = 0; i < uuids.length; i++) {
            writeUUID(uuids[i]);
        }
    }

    public void writeArray(boolean[] src) throws IOException {
        writeArray(src, 0, src.length);
    }

    public void writeArray(byte[] src) throws IOException {
        writeArray(src, 0, src.length);
    }

    public void writeArray(char[] src) throws IOException {
        writeArray(src, 0, src.length);
    }

    public void writeArray(short[] src) throws IOException {
        writeArray(src, 0, src.length);
    }

    public void writeArray(int[] src) throws IOException {
        writeArray(src, 0, src.length);
    }

    public void writeArray(long[] src) throws IOException {
        writeArray(src, 0, src.length);
    }

    public void writeArray(float[] src) throws IOException {
        writeArray(src, 0, src.length);
    }

    public void writeArray(double[] src) throws IOException {
        writeArray(src, 0, src.length);
    }

    public int readUnsignedShort() throws IOException {
        return readShort() & 0177777;
    }

    public int readUnsignedByte() throws IOException {
        return readByte() & 0377;
    }

    public void readArray(boolean[] src) throws IOException {
        readArray(src, 0, src.length);
    }

    public void readArray(byte[] src) throws IOException {
        readArray(src, 0, src.length);
    }

    public void readArray(char[] src) throws IOException {
        readArray(src, 0, src.length);
    }

    public void readArray(short[] src) throws IOException {
        readArray(src, 0, src.length);
    }

    public void readArray(int[] src) throws IOException {
        readArray(src, 0, src.length);
    }

    public void readArray(long[] src) throws IOException {
        readArray(src, 0, src.length);
    }

    public void readArray(float[] src) throws IOException {
        readArray(src, 0, src.length);
    }

    public void readArray(double[] src) throws IOException {
        readArray(src, 0, src.length);
    }

    public String readString() throws IOException {
        int length = readInt();

        if (length > payloadRemaining()) {
            throw new IOException("illegal string length (" + length
                    + ") specified, longer" + " than payload remaining");
        }

        byte[] bytes = new byte[length];
        readArray(bytes, 0, bytes.length);

        return new String(bytes, "UTF-8");
    }

    public String[] readStrings() throws IOException {
        String[] result = new String[readInt()];

        for (int i = 0; i < result.length; i++) {
            result[i] = readString();
        }

        return result;
    }

    public void readStringMap(Map<String, String> map) throws IOException {
        int size = readInt();

        for (int i = 0; i < size; i++) {
            map.put(readString(), readString());
        }
    }
    
    public Map<String, String> readStringMap() throws IOException {
        Map<String, String> result = new HashMap<String, String>();
        
        readStringMap(result);
        
        return result;
    }

    public URI readURI() throws IOException {
        try {
            return new URI(readString());
        } catch (URISyntaxException e) {
            throw new IOException("error in received URI: " + e);
        }
    }

    public URI[] readURIs() throws IOException {
        URI[] result = new URI[readInt()];

        for (int i = 0; i < result.length; i++) {
            result[i] = readURI();
        }

        return result;
    }

    public UUID readUUID() {
        long msb = payload.getLong();
        long lsb = payload.getLong();
        
        if (msb == 0L && lsb == 0L) {
            return null;
        } else {
            return new UUID(msb, lsb);
        }
        
    }

    public UUID[] readUUIDs() throws IOException {
        UUID[] result = new UUID[readInt()];

        for (int i = 0; i < result.length; i++) {
            result[i] = readUUID();
        }

        return result;
    }

    public void writeObject(Object object) throws IOException {
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(outBuffer);

        out.writeObject(object);
        out.close();

        byte[] bytes = outBuffer.toByteArray();

        logger.debug("writing object of " + bytes.length + " bytes to message");
        logger.debug("payload byteOrder = " + payload.order());

        writeInt(bytes.length);
        writeArray(bytes);
        outBuffer.close();
    }

    public Object readObject() throws IOException, ClassNotFoundException {
        int size = readInt();

        if (logger.isDebugEnabled()) {
            logger.debug("reading object of: " + size + " bytes");
            logger.debug("payload byteOrder = " + payload.order());
        }

        byte[] bytes = new byte[size];
        readArray(bytes);

        logger.debug("read bytes from stream");

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(bytes);

        if (logger.isDebugEnabled()) {
            logger.debug("wrapped bytes in stream, now: "
                    + inBuffer.available() + " bytes available");
        }

        ObjectInputStream in = new ObjectInputStream(inBuffer);

        logger.debug("reading object");

        Object result = in.readObject();

        in.close();
        inBuffer.close();

        return result;
    }

    public byte[] toBytes() {
        writeHeader();
        buffer.mark();

        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);

        if (buffer.hasRemaining()) {
            throw new ZorillaError("could not copy message contents to byte[]");
        }

        buffer.reset();

        return result;
    }

    public void setType(int type) {
        // TODO Auto-generated method stub
        
    }
}