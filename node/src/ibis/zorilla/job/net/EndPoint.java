package ibis.zorilla.job.net;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.MessageUpcall;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * @author Niels Drost
 * 
 * 
 * 
 */
public final class EndPoint implements MessageUpcall {
    /*
     * MESSAGE:
     *      int                     MESSAGE
     *      [PAYLOAD]
     *      
     * REQUEST:
     *      int                     REQUEST
     *      UUID                    callID
     *      ReceivePortIdentifier   replyPort
     *      [PAYLOAD]
     *
     * REPLY:
     *      int                     REPLY
     *      UUID                    callID
     *      int                     result (OK, EXCEPTION)
     *      [PAYLOAD] | [EXCEPTION]
     */ 
    

    
    Logger logger = Logger.getLogger(EndPoint.class);

    static final int MESSAGE = 0;
    static final int REQUEST = 1;
    static final int REPLY = 2;
    
    static final int OK = 0;

    static final int EXCEPTION = 1;

    private final Receiver receiver;

    private final ReceivePort receivePort;

    private Ibis ibis;

    public EndPoint(String name, Receiver reciever, Ibis ibis)
        throws IOException {
        this.receiver = reciever;
        this.ibis = ibis;

        // FIXME: use ibis local receiveports.
        receivePort = ibis.createReceivePort(Factory.callType, name, this);
        receivePort.enableConnections();
        receivePort.enableMessageUpcalls();

    }

    public Call call(ReceivePortIdentifier receiver) throws IOException {
        return new Call(receiver, ibis);
    }

    public Call call(IbisIdentifier target, String name) throws IOException {
        return new Call(target, name, ibis);
    }
    
    public WriteMessage send(ReceivePortIdentifier receiver) throws IOException {
        return new WriteMessage(receiver, ibis);
    }

    public void close() throws IOException {
        receivePort.close();
    }

    public ReceivePortIdentifier getID() {
        return receivePort.identifier();
    }

    public void upcall(ReadMessage m) throws IOException {
        logger.debug("reading in in upcall for opcode");

        int messageType = m.readInt();

        switch (messageType) {
        case MESSAGE:
            receiver.receive(m);
            break;
        case REQUEST:
            Invocation invocation = new Invocation(m, ibis);
            try {
                receiver.invoke(invocation);
                invocation.finish();
            } catch (IOException userException) {
                invocation.finish(new Exception(
                    "Exception on handling request", userException));
            } catch (Exception userException) {
                invocation.finish(userException);
            }
            break;
        default:
            logger.error("unknown message type received in upcall: "
                + messageType);
            break;
        }

    }

}
