package ibis.zorilla.monitor;

import ibis.zorilla.Node;
import ibis.zorilla.net.Message;
import ibis.zorilla.net.MessageHandler;
import ibis.zorilla.net.Network;

public class MonitoringModule implements MessageHandler {
    
    private Node node;
    private Network network;

    MonitoringModule(Node node, Network network) {
        this.node = node;
        this.network = network;
        
        network.register(this, Network.MONITORING_MODULE_ID);
    }

    @Override
    public void receive(Message message) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Message handleRequest(Message request) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }
    
    

}
