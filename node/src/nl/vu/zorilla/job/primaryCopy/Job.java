package nl.vu.zorilla.job.primaryCopy;

import ibis.ipl.CapabilitySet;
import ibis.ipl.Ibis;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import nl.vu.zorilla.job.net.EndPoint;
import nl.vu.zorilla.job.net.Receiver;

import static ibis.ipl.PredefinedCapabilities.*;

public abstract class Job extends nl.vu.zorilla.job.Job {

    final static String fileName(File file) throws IOException {
        String[] pathElements = file.getPath().split("/");

        if (pathElements.length == 0) {
            throw new IOException("could not find filename in given file"
                    + " path: " + file.getPath());
        }

        return pathElements[pathElements.length - 1];
    }

    final static Ibis createIbis(String pool) throws Exception {
        Ibis result;

        Properties properties = new Properties();

        properties.setProperty("ibis.registry.pool", pool);
        properties.setProperty("ibis.registry.impl",
                "ibis.impl.registry.NullRegistry");
        properties.setProperty("ibis.name", "tcp");

        CapabilitySet capabilities = new CapabilitySet(SERIALIZATION_OBJECT,
                WORLDMODEL_OPEN, COMMUNICATION_RELIABLE,
                CONNECTION_ONE_TO_MANY, CONNECTION_MANY_TO_ONE,
                CONNECTION_ONE_TO_ONE, RECEIVE_EXPLICIT, CONNECTION_UPCALLS,
                RECEIVE_TIMEOUT);

        try {
            result = IbisFactory.createIbis(capabilities, null, properties,
                    null);
        } catch (Exception e) {
            throw new Exception("cannot create ibis", e);
        }

        return result;
    }

    final static PortType createCallPortType(Ibis ibis) throws Exception {
        PortType result;

        CapabilitySet capabilities = new CapabilitySet(SERIALIZATION_OBJECT,
                COMMUNICATION_RELIABLE, CONNECTION_MANY_TO_ONE,
                CONNECTION_ONE_TO_ONE, CONNECTION_UPCALLS);

        try {
            result = ibis.createPortType(capabilities);

        } catch (Exception e) {
            throw new Exception("could not create port type", e);
        }
        return result;
    }

    final static PortType createReplyPortType(Ibis ibis) throws Exception {
        PortType result;

        CapabilitySet capabilities = new CapabilitySet(SERIALIZATION_OBJECT,
                COMMUNICATION_RELIABLE, CONNECTION_ONE_TO_ONE,
                RECEIVE_EXPLICIT, RECEIVE_TIMEOUT);

        try {
            result = ibis.createPortType(capabilities);

        } catch (Exception e) {
            throw new Exception("could not create port type", e);
        }
        return result;
    }

    public final String toString() {
        return getID().toString().substring(0, 7);
    }

    abstract void log(String message);

    abstract void log(String message, Exception exception);

    abstract EndPoint newEndPoint(String name, Receiver receiver)
            throws IOException, Exception;

    abstract IbisIdentifier getRandomConstituent();

}
