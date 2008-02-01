package nl.vu.zorilla.stats;

import java.io.IOException;

import org.apache.log4j.Logger;

import ibis.ipl.Ibis;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.StaticProperties;
import ibis.ipl.Upcall;
import nl.vu.zorilla.util.NameServer;

public class StatsGatherer implements Upcall {
    
    private static final Logger logger = Logger.getLogger(StatsGatherer.class);
    
    private final StatsHandler handler;
    private final Ibis ibis;

    public StatsGatherer(int port, StatsHandler handler) throws Exception {
        this.handler = handler;

        NameServer nameServer = new NameServer(System.out, port);

        // FIXME: HACK!
        System.setProperty("ibis.name_server.host", "localhost");
        System.setProperty("ibis.name_server.key", "zorilla");
        System.setProperty("ibis.name_server.port", Integer.toString(nameServer
                .port()));

        StaticProperties ibisProperties = new StaticProperties();

        ibisProperties.add("name", "tcp");
        ibisProperties.add("serialization", "object");
        ibisProperties.add("worldmodel", "open");
        ibisProperties.add("communication",
                "ManyToOne, Reliable, AutoUpcalls");

        ibis = Ibis.createIbis(ibisProperties, null);

        StaticProperties portProperties = new StaticProperties();

        portProperties.add("serialization", "object");
        portProperties.add("worldmodel", "open");
        portProperties.add("communication",
                "ManyToOne, Reliable, AutoUpcalls");

        PortType portType = ibis.createPortType("statistics", portProperties);

        ReceivePort receivePort = portType.createReceivePort("gatherer", this);
        receivePort.enableConnections();
        receivePort.enableUpcalls();
    }

    public void upcall(ReadMessage message) throws IOException {
        try {
            Stats stats = (Stats) message.readObject();
            
            handler.handle(stats);
        
        } catch (ClassNotFoundException e) {
            logger.error("could not handle stats message", e);
        }
    }
    
    
    public void end() throws IOException {
        ibis.end();
    }

  

}
