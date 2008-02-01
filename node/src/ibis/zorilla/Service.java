package ibis.zorilla;

import java.util.Map;

import ibis.smartsockets.direct.DirectSocket;

public interface Service {
    
    public void start() throws Exception;

    public void handleConnection(DirectSocket socket);
    
    public Map<String, String> getStats();
}
