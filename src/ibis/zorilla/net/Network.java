package ibis.zorilla.net;

import ibis.zorilla.Config;
import ibis.zorilla.Node;

public class Network extends Thread {

    public static final int MONITORING_MODULE_ID = 5;
    
    private final ConnectionSet superPeers;
    
    private final ConnectionSet neigbors;

    public Network(Node node, Config config) {

        setName("Zorilla Network");
        setDaemon(true);
    }

    // info for this node
    public NodeInfo getNodeInfo() {
        return null;
//            return new NodeInfo(id, name, config.getProperty(Config.CLUSTER_NAME),
//                    vivaldiService.getCoordinates(), network.getAddress(), version,
//                    config.isHub());
    }

    public SmartSocketsAddress getAddress() {
        return null;
    }

    // register a handler for a certain id
    public void register(MessageHandler handler, int handlerID) {

    }

    // register a handler, id assigned at random
    public int register(MessageHandler handler) {
        return 0;
    }

    public void deRegister(int handlerID) {

    }

    public void send(Message message, NodeInfo node, int moduleID) {
    }
    
    public void floodLatency(Message message, NodeInfo destination, int latency) {
    }
    
    public void floodHops(Message message, NodeInfo destination, int hops) {
    }


    public Message call(Message request, NodeInfo destination) throws Exception {
        return null;
    }

    public void end() {
        // TODO Auto-generated method stub

    }

    /**
     * Returns a (possibly new) connection to the given peer.
     * @param peer the peer node to connect to
     */
    public void getConnection(SmartSocketsAddress peer) throws NetworkException {
        // TODO Auto-generated method stub
        
    }

}
