package nl.vu.zorilla.jobNet;

import java.io.IOException;
import java.util.UUID;

import org.apache.log4j.Logger;

import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;
import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaException;
import nl.vu.zorilla.io.ObjectInput;
import nl.vu.zorilla.io.ObjectOutput;

public final class Call implements ObjectInput, ObjectOutput {
    
    Logger logger = Logger.getLogger(Call.class);
    
    private final UUID id;
    
    private SendPort sendPort;
    private WriteMessage request;

    private ReceivePort receivePort;
    private ReadMessage reply;

    Call(ReceivePortIdentifier destination, long timeout, PortType callType, PortType replyType) throws IOException {
        id = Node.generateUUID();
        
        logger.debug("creating call: " + id);
        
        sendPort = callType.createSendPort();
        receivePort = replyType.createReceivePort(Node.generateUUID().toString());
        receivePort.enableConnections();
        
        sendPort.connect(destination, timeout);

        request = sendPort.newMessage();
        
        writeInt(EndPoint.REQUEST);
        writeObject(id);
        writeObject(receivePort.identifier());
        
        reply = null;
    }
    
    public synchronized void call(long timeout) throws ZorillaException, IOException {
        logger.debug("doing call #" + id);

        if (request == null) {
            throw new ZorillaException("call already done");
        }
        
        logger.debug("finishing request, closing sendport, waiting for reply");
        
        request.finish();
        sendPort.close();
        //let go of handle to request and sendport
        request = null;
        sendPort = null;

        reply = receivePort.receive(timeout);

        if (reply == null) {
            throw new ZorillaException("reply not received");
        }
        
        try {
            int type = readInt();
            UUID replyID = (UUID) readObject();
            
            if (type != EndPoint.REPLY) {
                throw new ZorillaException("unknown reply message type: " + type);
            }
            if (!replyID.equals(id)) {
                throw new ZorillaException("reply received for unknown call");
            }
            
        } catch (ClassNotFoundException e) {
            throw new ZorillaException("unknown class in reading reply", e);
        }
        
        int result = readInt();
        
        logger.debug("reply (" + result + ") recieved");
        
        if (result == EndPoint.OK) {
            return;
        } else if (result == EndPoint.EXCEPTION) {
            ZorillaException exception;
            try {
            exception = (ZorillaException) reply.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException("unknown class in reading excepton: " + e);
            }
            logger.debug("exception in result: " + exception);
            
            throw new ZorillaException("REMOTE exception received", exception);
        } else {
            throw new IOException("unknown result in reply message: " + result);
        }
    }
    
    public synchronized void finish() throws IOException {
        if (request != null) {
            throw new IOException("call finished while request not done");
        }
        if (reply == null) {
            logger.debug("call finished twice");
            return;
        }
        reply.finish();
        reply = null;
    }

    private synchronized WriteMessage getRequest() throws IOException {
        if (request == null) {
            throw new IOException("writing to request while call already done");
        }
    //    logger.debug("writing", new Exception());
        return request;
    }
    
    private synchronized ReadMessage getReply() throws IOException {
        if (reply == null) {
            throw new IOException("trying to read reply while call" +
                    " not done yet");
        }
    //    logger.debug("reading", new Exception());
        return reply;
    }

    UUID getID() {
        return id;
    }
    
    public void writeString(String val) throws IOException {
        getRequest().writeString(val);
    }

    public void writeObject(Object object) throws IOException {
        getRequest().writeObject(object);
    }

    public void writeBoolean(boolean value) throws IOException {
        getRequest().writeBoolean(value);
    }

    public void writeByte(byte value) throws IOException {
        getRequest().writeByte(value);
    }

    public void writeChar(char value) throws IOException {
        getRequest().writeChar(value);
    }

    public void writeShort(short value) throws IOException {
        getRequest().writeShort(value);
    }

    public void writeInt(int value) throws IOException {
        getRequest().writeInt(value);
    }

    public void writeLong(long value) throws IOException {
        getRequest().writeLong(value);
    }

    public void writeFloat(float value) throws IOException {
        getRequest().writeFloat(value);
    }

    public void writeDouble(double value) throws IOException {
        getRequest().writeDouble(value);
    }

    public void writeArray(boolean[] source, int offset, int length) throws IOException {
        getRequest().writeArray(source, offset, length);
    }

    public void writeArray(byte[] source, int offset, int length) throws IOException {
        getRequest().writeArray(source, offset, length);
    }

    public void writeArray(char[] source, int offset, int length) throws IOException {
        getRequest().writeArray(source, offset, length);
    }

    public void writeArray(short[] source, int offset, int length) throws IOException {
        getRequest().writeArray(source, offset, length);
    }

    public void writeArray(int[] source, int offset, int length) throws IOException {
        getRequest().writeArray(source, offset, length);
    }

    public void writeArray(long[] source, int offset, int length) throws IOException {
        getRequest().writeArray(source, offset, length);
    }

    public void writeArray(float[] source, int offset, int length) throws IOException {
        getRequest().writeArray(source, offset, length);
    }

    public void writeArray(double[] source, int offset, int length) throws IOException {
        getRequest().writeArray(source, offset, length);
    }

    public void writeArray(boolean[] source) throws IOException {
        getRequest().writeArray(source);
    }

    public void writeArray(byte[] source) throws IOException {
        getRequest().writeArray(source);
    }

    public void writeArray(char[] source) throws IOException {
        getRequest().writeArray(source);
    }

    public void writeArray(short[] source) throws IOException {
        getRequest().writeArray(source);
    }

    public void writeArray(int[] source) throws IOException {
        getRequest().writeArray(source);
    }

    public void writeArray(long[] source) throws IOException {
        getRequest().writeArray(source);
    }

    public void writeArray(float[] source) throws IOException {
        getRequest().writeArray(source);
    }

    public void writeArray(double[] source) throws IOException {
        getRequest().writeArray(source);
    }
    
    public String readString() throws IOException {
        return getReply().readString();
    }

    public Object readObject() throws IOException, ClassNotFoundException {
        return getReply().readObject();
    }

    public boolean readBoolean() throws IOException {
        return getReply().readBoolean();
    }

    public byte readByte() throws IOException {
        return getReply().readByte();
    }

    public char readChar() throws IOException {
        return getReply().readChar();
    }

    public short readShort() throws IOException {
        return getReply().readShort();
    }

    public int readInt() throws IOException {
        return getReply().readInt();
    }

    public int readUnsignedByte() throws IOException {
        //FIXME: implement
        throw new IOException("read unsigned byte not implemented");
    }

    public int readUnsignedShort() throws IOException {
        //FIXME: implement
        throw new IOException("read unsigned short not implemented");
    }

    public long readLong() throws IOException {
        return getReply().readLong();
    }

    public float readFloat() throws IOException {
        return getReply().readFloat();
    }

    public double readDouble() throws IOException {
        return getReply().readDouble();
    }

    public void readArray(boolean[] destination, int offset, int length) throws IOException {
        getReply().readArray(destination, offset, length);
    }

    public void readArray(byte[] destination, int offset, int length) throws IOException {
        getReply().readArray(destination, offset, length);
    }

    public void readArray(char[] destination, int offset, int length) throws IOException {
        getReply().readArray(destination, offset, length);
    }

    public void readArray(short[] destination, int offset, int length) throws IOException {
        getReply().readArray(destination, offset, length);
    }

    public void readArray(int[] destination, int offset, int length) throws IOException {
        getReply().readArray(destination, offset, length);
    }

    public void readArray(long[] destination, int offset, int length) throws IOException {
        getReply().readArray(destination, offset, length);
    }

    public void readArray(float[] destination, int offset, int length) throws IOException {
        getReply().readArray(destination, offset, length);
    }

    public void readArray(double[] destination, int offset, int length) throws IOException {
        getReply().readArray(destination, offset, length);
    }

    public void readArray(boolean[] source) throws IOException {
        getReply().readArray(source);
    }

    public void readArray(byte[] source) throws IOException {
        getReply().readArray(source);
    }

    public void readArray(char[] source) throws IOException {
        getReply().readArray(source);
    }

    public void readArray(short[] source) throws IOException {
        getReply().readArray(source);
    }

    public void readArray(int[] source) throws IOException {
        getReply().readArray(source);
    }

    public void readArray(long[] source) throws IOException {
        getReply().readArray(source);
    }

    public void readArray(float[] source) throws IOException {
        getReply().readArray(source);
    }

    public void readArray(double[] source) throws IOException {
        getReply().readArray(source);
    }

}
