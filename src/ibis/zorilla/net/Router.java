package ibis.zorilla.net;


/**
 * Router class to make all routing decisions. Also keeps track of all known
 * nodes.
 * 
 * @author Niels Drost
 * 
 */
public class Router {

    /**
     * Returns the newest known info for the given node. Updates the newest
     * version if the info passed to this function happens to be newer than the
     * currently known info.
     * 
     * @param info
     *            the node for which to fetch the newest info
     * 
     * @return the newest known info for the given node. May be the exact same
     *         object passed as a parameter.
     */
    public NodeInfo updateNodeInfo(NodeInfo info) {
        return null;
    }
    
    /**
     * Routes the the given message to one (or more, or none) of the connections.
     * 
     * @param message the message that needs to be routed
     * @param connections the possible connections to send the message to.
     * 
     * @throws RoutingException
     */
    public void route(Message message, Connection[] connections) throws RoutingException {
        
    }
}
