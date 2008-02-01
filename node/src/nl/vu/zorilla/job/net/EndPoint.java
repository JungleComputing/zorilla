package nl.vu.zorilla.job.net;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.Upcall;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * @author Niels Drost
 * 
 * 
 * 
 */
public final class EndPoint implements Upcall {
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

    PortType callType;
    PortType replyType;

    public EndPoint(String name, Receiver reciever, PortType callType, PortType replyType)
        throws IOException {
        this.receiver = reciever;
        this.callType = callType;
        this.replyType = replyType;

        // FIXME: use ibis local receiveports.
        receivePort = callType.createReceivePort(name, this);
        receivePort.enableConnections();
        receivePort.enableUpcalls();

    }

    public Call call(ReceivePortIdentifier receiver) throws IOException {
        return new Call(receiver, callType, replyType);
    }

    public Call call(IbisIdentifier ibis, String name) throws IOException {
        return new Call(ibis, name, callType, replyType);
    }
    
    public WriteMessage send(ReceivePortIdentifier receiver) throws IOException {
        return new WriteMessage(receiver, callType);
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
            Invocation invocation = new Invocation(m, replyType);
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
