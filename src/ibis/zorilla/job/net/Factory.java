package ibis.zorilla.job.net;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisProperties;
import ibis.ipl.PortType;
import ibis.smartsockets.direct.DirectSocketAddress;
import ibis.zorilla.Node;

import java.awt.Color;
import java.util.Properties;

public class Factory {

    public static final long CONNECTION_TIMEOUT = 60 * 1000;

    public static final PortType callType = new PortType(
            PortType.SERIALIZATION_OBJECT, PortType.COMMUNICATION_RELIABLE,
            PortType.CONNECTION_LIGHT, PortType.CONNECTION_MANY_TO_ONE,
            PortType.RECEIVE_AUTO_UPCALLS);

    public static final PortType replyType = new PortType(
            PortType.SERIALIZATION_OBJECT, PortType.COMMUNICATION_RELIABLE,
            PortType.CONNECTION_LIGHT, PortType.CONNECTION_ONE_TO_ONE,
            PortType.RECEIVE_EXPLICIT, PortType.RECEIVE_TIMEOUT);

    public final static Ibis createIbis(String pool, Node node)
            throws Exception {
        Ibis result;

        Properties properties = new Properties();

        properties.setProperty(IbisProperties.POOL_NAME, pool);
        properties.setProperty(IbisProperties.REGISTRY_IMPLEMENTATION, "null");
        properties.setProperty(IbisProperties.IMPLEMENTATION, "smartsockets");

        properties.setProperty(IbisProperties.LOCATION_COLOR, ""
                + Color.BLACK.getRGB());

        DirectSocketAddress hubAddress = node.network().getAddress().hub();
        if (hubAddress != null) {
            properties.setProperty(IbisProperties.HUB_ADDRESSES, ""
                    + hubAddress);
        }

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
