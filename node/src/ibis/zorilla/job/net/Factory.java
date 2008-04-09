package ibis.zorilla.job.net;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisProperties;
import ibis.ipl.PortType;

import java.util.Properties;

public class Factory {
	
	public static final long CONNECTION_TIMEOUT = 60 * 1000;

    public static final PortType callType = new PortType(
            PortType.SERIALIZATION_OBJECT, PortType.COMMUNICATION_RELIABLE,
            PortType.CONNECTION_MANY_TO_ONE,
            PortType.RECEIVE_AUTO_UPCALLS);

    public static final PortType replyType = new PortType(
            PortType.SERIALIZATION_OBJECT, PortType.COMMUNICATION_RELIABLE,
            PortType.CONNECTION_ONE_TO_ONE, PortType.RECEIVE_EXPLICIT,
            PortType.RECEIVE_TIMEOUT);

    public final static Ibis createIbis(String pool) throws Exception {
        Ibis result;

        Properties properties = new Properties();

        properties.setProperty(IbisProperties.POOL_NAME, pool);
        properties.setProperty(IbisProperties.REGISTRY_IMPLEMENTATION,
                "ibis.ipl.impl.registry.NullRegistry");
        properties.setProperty(IbisProperties.IMPLEMENTATION, "tcp");

        // no capabilities needed from ibis
        IbisCapabilities capabilities = new IbisCapabilities();

        try {
            result = IbisFactory.createIbis(capabilities, properties, true,
                    null, callType, replyType);
        } catch (Exception e) {
            throw new Exception("cannot create ibis", e);
        }

        return result;
    }
}
