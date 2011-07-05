package ibis.zorilla.net;

public interface MessageHandler {
    
    void receive(Message message);
    
    Message handleRequest(Message request) throws Exception;

}
