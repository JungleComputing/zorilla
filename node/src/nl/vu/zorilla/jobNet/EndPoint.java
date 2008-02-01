package nl.vu.zorilla.jobNet;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.Upcall;

import java.io.IOException;

import nl.vu.zorilla.Config;
import nl.vu.zorilla.ZorillaException;

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

    private final Ibis ibis;
    
    PortType callType;
    PortType replyType;

    public EndPoint(String name, Receiver reciever, Ibis ibis, PortType callType, PortType replyType)
        throws IOException {
        this.receiver = reciever;
        this.ibis = ibis;
        this.callType = callType;
        this.replyType = replyType;

        // FIXME: use ibis local receiveports.
        receivePort = callType.createReceivePort(name + " on " + ibis.name(), this);
        receivePort.enableConnections();
        receivePort.enableUpcalls();

    }

    public Call call(ReceivePortIdentifier receiver) throws IOException {
        return new Call(receiver, Config.CALL_TIMEOUT, callType, replyType);
    }

    public Call call(IbisIdentifier ibisIdentifier, String name)
        throws IOException {
        // FIXME: use ibis local receiveports
        return call(ibis.registry().lookupReceivePort(
            name + " on " + ibisIdentifier.name()));
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
                invocation.finish(new ZorillaException(
                    "Exception on handling request", userException));
            } catch (ZorillaException userException) {
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
