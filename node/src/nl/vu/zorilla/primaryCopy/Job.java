package nl.vu.zorilla.primaryCopy;

import ibis.ipl.Ibis;
import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.StaticProperties;

import java.io.File;
import java.io.IOException;

import nl.vu.zorilla.ZorillaException;
import nl.vu.zorilla.jobNet.EndPoint;
import nl.vu.zorilla.jobNet.Receiver;

public abstract class Job extends nl.vu.zorilla.Job {

    final static String fileName(File file) throws IOException {
        String[] pathElements = file.getPath().split("/");

        if (pathElements.length == 0) {
            throw new IOException("could not find filename in given file"
                + " path: " + file.getPath());
        }

        return pathElements[pathElements.length - 1];
    }

    final static Ibis createIbis(String host, int port, String key)
        throws ZorillaException {
        Ibis result;

        // FIXME: HACK!
        System.setProperty("ibis.name_server.host", host);
        System.setProperty("ibis.name_server.key", key);
        System.setProperty("ibis.name_server.port", Integer.toString(port));

        StaticProperties reqprops = new StaticProperties();

        reqprops.add("name", "tcp");
        reqprops.add("serialization", "sun");
        reqprops.add("worldmodel", "open");
        reqprops.add("communication",
            "OneToOne, ManyToOne, Reliable, AutoUpcalls, ExplicitReceipt");

        try {
            result = Ibis.createIbis(reqprops, null);
        } catch (IbisException e) {
            throw new ZorillaException("cannot create ibis", e);
        }

        return result;
    }

    final static PortType createCallPortType(Ibis ibis) throws ZorillaException {
        PortType result;

        StaticProperties reqprops = new StaticProperties();

        reqprops.add("serialization", "object");
        reqprops.add("communication",
            "OneToOne, ManyToOne, Reliable, AutoUpcalls");

        try {
            result = ibis.createPortType("Zorilla", reqprops);

        } catch (Exception e) {
            throw new ZorillaException("could not create port type", e);
        }
        return result;
    }

    final static PortType createReplyPortType(Ibis ibis) throws ZorillaException {
        PortType result;

        StaticProperties reqprops = new StaticProperties();

        reqprops.add("serialization", "object");
        reqprops.add("communication",
            "OneToOne, Reliable, ExplicitReceipt");

        try {
            result = ibis.createPortType("Zorilla", reqprops);

        } catch (Exception e) {
            throw new ZorillaException("could not create port type", e);
        }
        return result;
    }

    public final String toString() {
        return getID().toString().substring(0, 7);
    }
    
    abstract void log(String message);
    
    abstract void log(String message, Exception exception);

    abstract EndPoint newEndPoint(String name, Receiver receiver) throws IOException, ZorillaException;

    abstract IbisIdentifier getRandomConstituent(); 
  

    
    
}
