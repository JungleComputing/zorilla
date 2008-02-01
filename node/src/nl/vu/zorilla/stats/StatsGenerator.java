package nl.vu.zorilla.stats;

import org.apache.log4j.Logger;

import smartsockets.direct.SocketAddressSet;

import ibis.ipl.Ibis;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.StaticProperties;
import ibis.ipl.WriteMessage;
import ibis.util.ThreadPool;
import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaException;

public class StatsGenerator implements Runnable {
    
    private static final Logger logger = Logger.getLogger(StatsGenerator.class);

    private static final int TIMEOUT = 10 * 1000;

    private final Ibis ibis;

    private final SendPort sendPort;

    private final Node node;

    private boolean ended;

    public StatsGenerator(Node node, SocketAddressSet homeNodeAddress)
            throws ZorillaException {
        this.node = node;

        try {

            // Create Ibis
            
            //FIXME: broken
            
/*            System.setProperty("ibis.util.ip.address", node.guessInetAddress().getHostAddress());
            System.setProperty("ibis.util.ip.alt-address", node.guessInetAddress().getHostAddress());
            System.setProperty("ibis.name_server.host", homeNodeAddress
                    .getAddress().getHostAddress());
            System.setProperty("ibis.name_server.key", "zorilla");
            System.setProperty("ibis.name_server.port", Integer
                    .toString(homeNodeAddress.getPort()));
*/
            StaticProperties ibisProperties = new StaticProperties();

            ibisProperties.add("name", "tcp");
            ibisProperties.add("serialization", "object");
            ibisProperties.add("worldmodel", "open");
            ibisProperties.add("communication",
                    "ManyToOne, Reliable, AutoUpcalls");

            logger.debug("creating ibis");
            
            ibis = Ibis.createIbis(ibisProperties, null);

            StaticProperties portProperties = new StaticProperties();

            portProperties.add("serialization", "object");
            portProperties.add("worldmodel", "open");
            portProperties.add("communication",
                    "ManyToOne, Reliable, AutoUpcalls");


            PortType portType = ibis.createPortType("statistics",
                    portProperties);

            logger.debug("creating sendport");
            
            sendPort = portType.createSendPort();

            logger.debug("looking up receiveport");
            
            ReceivePortIdentifier gatherPort = ibis.registry()
                    .lookupReceivePort("gatherer");
            
            logger.debug("connecting to " + gatherPort);

            sendPort.connect(gatherPort);
            
            logger.debug("ibis initialized");

            ThreadPool.createNew(this, "stats generator");

        } catch (Exception e) {
            throw new ZorillaException("could not create stats generator", e);
        }

    }

    public synchronized void stop() {
        ended = true;
        notifyAll();
    }

    public void run() {
        try {

            while (true) {
                logger.debug("sending out statistics");
                Stats stats = node.getStats();

                WriteMessage message = sendPort.newMessage();

                message.writeObject(stats);

                message.finish();
                
                logger.debug("done sending");

                synchronized (this) {
                    if (ended) {
                        ibis.end();
                        return;
                    }
                    try {
                        wait(TIMEOUT);
                    } catch (InterruptedException e) {
                        // IGNORE
                    }
                    if (ended) {
                        ibis.end();
                        return;
                    }
                }
            }

        } catch (Exception e) {
            logger.error("could not send statistics to home node", e);
        }
    }

}
