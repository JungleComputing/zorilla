package nl.vu.zorilla;

import java.util.Map;

import smartsockets.direct.DirectSocket;

public interface Service {
    
    public void start() throws Exception;

    public void handleConnection(DirectSocket socket);
    
    public Map<String, String> getStats();
}
