package ibis.zorilla.job.net;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;
import ibis.zorilla.io.ObjectInput;
import ibis.zorilla.io.ObjectOutput;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import org.apache.log4j.Logger;



public class Invocation extends InputStream implements ObjectInput, ObjectOutput {

    public static final long CONNECTION_TIMEOUT = 10 * 1000;
    
    private static int READING = 0;

    private static int FINISHED_READING = 1;

    private static int WRITING = 2;

    private static int DONE = 3;

    private static final Logger logger = Logger.getLogger(Invocation.class);

    private ReadMessage request;
    
    private final Ibis ibis;

    private SendPort sendPort;

    private WriteMessage reply;

    private UUID id;

    int state = READING;

    private final ReceivePortIdentifier caller;

    public Invocation(ReadMessage request, Ibis ibis) throws IOException {
        this.request = request;
        this.ibis = ibis;

        reply = null;
        
        try {
            id = (UUID) readObject();
            caller = (ReceivePortIdentifier) readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("error on reading request info" + e);
        }

        logger.debug("invocation #" + id + " received");
    }

    public synchronized void finishRead() throws IOException {
        logger.debug("finishing read");

        if (state != READING) {
            throw new IOException("finishing read while not reading");
        }
        request.finish();
        request = null;
        state = FINISHED_READING;
    }

    public synchronized void startReply() throws IOException {
        logger.debug("starting reply");

        if (state == READING) {
            finishRead();
        }

        if (state != FINISHED_READING) {
            throw new IOException("reply already started");
        }

        sendPort = ibis.createSendPort(Factory.replyType);
        sendPort.connect(caller, CONNECTION_TIMEOUT, true);

        reply = sendPort.newMessage();
        state = WRITING;
        writeInt(EndPoint.REPLY);
        writeObject(id);
        writeInt(EndPoint.OK);
    }

    public synchronized void finish() throws IOException {
        logger.debug("finishing invocation #" + id);

        if (state == READING || state == FINISHED_READING) {
            logger.debug("creating reply message in finish");
            startReply();
        }

        if (state == DONE) {
            logger.debug("invocation already finished");
            return;
        }

        if (state != WRITING) {
            throw new IOException("unknown state " + state
                + " in invocation.finish()");
        }

        reply.finish();
        reply = null;
        //FIXME: why was there a sleep here?
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            logger.debug("sleep interrupted");
//        }
        sendPort.close();
        sendPort = null;
        state = DONE;
    }

    synchronized void finish(Exception userException) {
        logger.debug("finishing invocation #" + id + " with exception: "
            + userException);

        if (state == DONE) {
            logger.debug("invocation already done");
            return;
        }

        try {
            if (state == READING) {
                finishRead();
            }

            if (state == FINISHED_READING) {
                // send special "exception" message to caller
                sendPort = ibis.createSendPort(Factory.replyType);
                sendPort.connect(caller, CONNECTION_TIMEOUT, true);
                WriteMessage reply = sendPort.newMessage();
                reply.writeInt(EndPoint.REPLY);
                reply.writeObject(id);
                reply.writeInt(EndPoint.EXCEPTION);
                reply.writeObject(userException);
                reply.finish();
                sendPort.close();
                sendPort = null;
                state = DONE;
            } else if (state == WRITING) {
                // give exception to existing reply
                reply.finish(new IOException(userException.getMessage()));
            }
        } catch (IOException e) {
            logger.error("could not finish invocation (with error)", e);
        }
    }

    public IbisIdentifier getSourceIbis() {
        return caller.ibisIdentifier();
    }

    public ReceivePortIdentifier getCaller() {
        return caller;
    }

    private synchronized ReadMessage getRequest() throws IOException {
        if (state != READING) {
            throw new IOException(
                "reading from request but reading already finished");
        }
      //  logger.debug("reading", new Exception());
        return request;
    }

    private synchronized WriteMessage getReply() throws IOException {
        if (state == READING || state == FINISHED_READING) {
            startReply();
        }

        if (state != WRITING) {
            throw new IOException("writing to reply while not in writing state");
        }
        
     //   logger.debug("writing", new Exception()); 

        return reply;
    }

    public String readString() throws IOException {
        return getRequest().readString();
    }

    public Object readObject() throws IOException, ClassNotFoundException {
        return getRequest().readObject();
    }

    public boolean readBoolean() throws IOException {
        return getRequest().readBoolean();
    }

    public byte readByte() throws IOException {
        return getRequest().readByte();
    }

    public char readChar() throws IOException {
        return getRequest().readChar();
    }

    public short readShort() throws IOException {
        return getRequest().readShort();
    }

    public int readInt() throws IOException {
        return getRequest().readInt();
    }

    public int readUnsignedByte() throws IOException {
        // FIXME: implement
        throw new IOException("not implemented");
    }

    public int readUnsignedShort() throws IOException {
        // FIXME: implement
        throw new IOException("not implemented");
    }

    public long readLong() throws IOException {
        return getRequest().readLong();
    }

    public float readFloat() throws IOException {
        return getRequest().readFloat();
    }

    public double readDouble() throws IOException {
        return getRequest().readDouble();
    }

    public void readArray(boolean[] destination, int offset, int length)
        throws IOException {
        getRequest().readArray(destination, offset, length);
    }

    public void readArray(byte[] destination, int offset, int length)
        throws IOException {
        getRequest().readArray(destination, offset, length);
    }

    public void readArray(char[] destination, int offset, int length)
        throws IOException {
        getRequest().readArray(destination, offset, length);
    }

    public void readArray(short[] destination, int offset, int length)
        throws IOException {
        getRequest().readArray(destination, offset, length);
    }

    public void readArray(int[] destination, int offset, int length)
        throws IOException {
        getRequest().readArray(destination, offset, length);
    }

    public void readArray(long[] destination, int offset, int length)
        throws IOException {
        getRequest().readArray(destination, offset, length);
    }

    public void readArray(float[] destination, int offset, int length)
        throws IOException {
        getRequest().readArray(destination, offset, length);
    }

    public void readArray(double[] destination, int offset, int length)
        throws IOException {
        getRequest().readArray(destination, offset, length);
    }

    public void readArray(boolean[] source) throws IOException {
        getRequest().readArray(source);
    }

    public void readArray(byte[] source) throws IOException {
        getRequest().readArray(source);
    }

    public void readArray(char[] source) throws IOException {
        getRequest().readArray(source);
    }

    public void readArray(short[] source) throws IOException {
        getRequest().readArray(source);
    }

    public void readArray(int[] source) throws IOException {
        getRequest().readArray(source);
    }

    public void readArray(long[] source) throws IOException {
        getRequest().readArray(source);
    }

    public void readArray(float[] source) throws IOException {
        getRequest().readArray(source);
    }

    public void readArray(double[] source) throws IOException {
        getRequest().readArray(source);
    }

    public void writeString(String val) throws IOException {
        getReply().writeString(val);
    }

    public void writeObject(Object object) throws IOException {
        getReply().writeObject(object);
    }

    public void writeBoolean(boolean value) throws IOException {
        getReply().writeBoolean(value);
    }

    public void writeByte(byte value) throws IOException {
        getReply().writeByte(value);
    }

    public void writeChar(char value) throws IOException {
        getReply().writeChar(value);
    }

    public void writeShort(short value) throws IOException {
        getReply().writeShort(value);
    }

    public void writeInt(int value) throws IOException {
        getReply().writeInt(value);
    }

    public void writeLong(long value) throws IOException {
        getReply().writeLong(value);
    }

    public void writeFloat(float value) throws IOException {
        getReply().writeFloat(value);
    }

    public void writeDouble(double value) throws IOException {
        getReply().writeDouble(value);
    }

    public void writeArray(boolean[] source, int offset, int length)
        throws IOException {
        getReply().writeArray(source, offset, length);
    }

    public void writeArray(byte[] source, int offset, int length)
        throws IOException {
        getReply().writeArray(source, offset, length);
    }

    public void writeArray(char[] source, int offset, int length)
        throws IOException {
        getReply().writeArray(source, offset, length);
    }

    public void writeArray(short[] source, int offset, int length)
        throws IOException {
        getReply().writeArray(source, offset, length);
    }

    public void writeArray(int[] source, int offset, int length)
        throws IOException {
        getReply().writeArray(source, offset, length);
    }

    public void writeArray(long[] source, int offset, int length)
        throws IOException {
        getReply().writeArray(source, offset, length);
    }

    public void writeArray(float[] source, int offset, int length)
        throws IOException {
        getReply().writeArray(source, offset, length);
    }

    public void writeArray(double[] source, int offset, int length)
        throws IOException {
        getReply().writeArray(source, offset, length);
    }

    public void writeArray(boolean[] source) throws IOException {
        getReply().writeArray(source);
    }

    public void writeArray(byte[] source) throws IOException {
        getReply().writeArray(source);
    }

    public void writeArray(char[] source) throws IOException {
        getReply().writeArray(source);
    }

    public void writeArray(short[] source) throws IOException {
        getReply().writeArray(source);
    }

    public void writeArray(int[] source) throws IOException {
        getReply().writeArray(source);
    }

    public void writeArray(long[] source) throws IOException {
        getReply().writeArray(source);
    }

    public void writeArray(float[] source) throws IOException {
        getReply().writeArray(source);
    }

    public void writeArray(double[] source) throws IOException {
        getReply().writeArray(source);
    }
    
    public void flush() throws IOException {
        getReply().sync(0);
    }
    
    public String toString() {
        return "invocation from " + caller + " to " + request.localPort();
    }

    @Override
    public int read() throws IOException {
        byte[] byteArray = new byte[1];
        
        read(byteArray);
        
        return byteArray[0];
        
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        readArray(b, off, len);
        return len;
    }

    @Override
    public int read(byte[] b) throws IOException {
        readArray(b, 0, b.length);
        return b.length;
    }

}
