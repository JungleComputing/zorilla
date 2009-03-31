package ibis.zorilla.job.net;

import ibis.ipl.Ibis;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.zorilla.io.ObjectOutput;

import java.io.IOException;

public class WriteMessage implements ObjectOutput {

    private ibis.ipl.WriteMessage message;
    private SendPort sendPort;

    public WriteMessage(ReceivePortIdentifier receiver, Ibis ibis)
            throws IOException {
        sendPort = ibis.createSendPort(Factory.callType);
        sendPort.connect(receiver, Factory.CONNECTION_TIMEOUT, true);
        message = sendPort.newMessage();
        message.writeInt(EndPoint.MESSAGE);
    }

    public void finish() throws IOException {
        message.finish();
        sendPort.close();
    }

    public void writeString(String val) throws IOException {
        message.writeString(val);
    }

    public void writeObject(Object object) throws IOException {
        message.writeObject(object);
    }

    public void writeBoolean(boolean value) throws IOException {
        message.writeBoolean(value);
    }

    public void writeByte(byte value) throws IOException {
        message.writeByte(value);
    }

    public void writeChar(char value) throws IOException {
        message.writeChar(value);
    }

    public void writeShort(short value) throws IOException {
        message.writeShort(value);
    }

    public void writeInt(int value) throws IOException {
        message.writeInt(value);
    }

    public void writeLong(long value) throws IOException {
        message.writeLong(value);
    }

    public void writeFloat(float value) throws IOException {
        message.writeFloat(value);
    }

    public void writeDouble(double value) throws IOException {
        message.writeDouble(value);
    }

    public void writeArray(boolean[] source, int offset, int length)
            throws IOException {
        message.writeArray(source, offset, length);
    }

    public void writeArray(byte[] source, int offset, int length)
            throws IOException {
        message.writeArray(source, offset, length);
    }

    public void writeArray(char[] source, int offset, int length)
            throws IOException {
        message.writeArray(source, offset, length);
    }

    public void writeArray(short[] source, int offset, int length)
            throws IOException {
        message.writeArray(source, offset, length);
    }

    public void writeArray(int[] source, int offset, int length)
            throws IOException {
        message.writeArray(source, offset, length);
    }

    public void writeArray(long[] source, int offset, int length)
            throws IOException {
        message.writeArray(source, offset, length);
    }

    public void writeArray(float[] source, int offset, int length)
            throws IOException {
        message.writeArray(source, offset, length);
    }

    public void writeArray(double[] source, int offset, int length)
            throws IOException {
        message.writeArray(source, offset, length);
    }

    public void writeArray(boolean[] source) throws IOException {
        message.writeArray(source);
    }

    public void writeArray(byte[] source) throws IOException {
        message.writeArray(source);
    }

    public void writeArray(char[] source) throws IOException {
        message.writeArray(source);
    }

    public void writeArray(short[] source) throws IOException {
        message.writeArray(source);
    }

    public void writeArray(int[] source) throws IOException {
        message.writeArray(source);
    }

    public void writeArray(long[] source) throws IOException {
        message.writeArray(source);
    }

    public void writeArray(float[] source) throws IOException {
        message.writeArray(source);
    }

    public void writeArray(double[] source) throws IOException {
        message.writeArray(source);
    }

}
