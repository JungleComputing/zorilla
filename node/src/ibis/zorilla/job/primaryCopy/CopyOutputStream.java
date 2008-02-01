package ibis.zorilla.job.primaryCopy;

import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePortIdentifier;
import ibis.zorilla.job.net.Call;
import ibis.zorilla.job.net.EndPoint;
import ibis.zorilla.job.net.Invocation;
import ibis.zorilla.job.net.Receiver;
import ibis.zorilla.job.net.WriteMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.log4j.Logger;


final class CopyOutputStream extends OutputStream implements Receiver {
    
    public static final int BUFFER_SIZE = 100 * 1024;

    private static final Logger logger = Logger
            .getLogger(CopyOutputStream.class);

    private final String virtualPath;

    private final ReceivePortIdentifier primary;

    private final Copy copy;

    private final EndPoint endPoint;

    public CopyOutputStream(Call call, Copy copy) throws IOException,
            Exception {
        try {

        this.copy = copy;
        
        virtualPath = call.readString();
        UUID id = (UUID) call.readObject();
        primary = (ReceivePortIdentifier) call.readObject();
        
        endPoint = copy.newEndPoint(id.toString(), this);
        
        } catch (ClassNotFoundException e) {
            throw new Exception("could not read bootstrap from call", e);
        }

    }

    public synchronized void write(byte[] data, int offset, int length)
            throws IOException {
            logger.debug("sending data to " + primary);

            WriteMessage message = endPoint.send(primary);

            logger
                    .debug("writing " + length + " bytes to file: "
                            + virtualPath);

            //stream data to other side
            while (length >0) {
                int maxWrite = Math.min(length, BUFFER_SIZE);

                message.writeInt(maxWrite);
                message.writeArray(data, offset, maxWrite);
                offset+= maxWrite;
                length -= maxWrite;
            }
            
            message.writeInt(-1);
            message.finish();
            logger.debug("writing done");
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(int b) throws IOException {
        byte[] byteArray = new byte[1];
        
        byteArray[0] = (byte) b;
        
        write(byteArray, 0, byteArray.length);
    }

    @Override
    public void flush() throws IOException {
        //NOTHING
    }
    
    public void close() throws IOException {
        endPoint.close();
    }


    public void receive(ReadMessage message) {
        Exception e = new Exception("message received in copy file");
        copy.log(e.getMessage(), e);
    }

    public void invoke(Invocation invocation) throws Exception {
        throw new Exception("cannot invoke copy output file");
    }

    public void readFrom(InputStream data) throws IOException {
        logger.debug("sending data to " + primary);

        WriteMessage message = endPoint.send(primary);

        //stream data to other side
        byte[] buffer = new byte[BUFFER_SIZE];
        while (true) {
            int read = data.read(buffer);
            
            if (read == -1) {
                data.close();
                message.writeInt(-1);
                message.finish();
                logger.debug("writing done");
                return;
            }
            
            logger.debug("writing " + read + " bytes to primary");
            
            message.writeInt(read);
            message.writeArray(buffer, 0, read);
        }
        
    }

}
