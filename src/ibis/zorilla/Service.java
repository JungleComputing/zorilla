package ibis.zorilla;

import java.util.Map;

import ibis.smartsockets.virtual.VirtualSocket;

public interface Service {

    public void start() throws Exception;

    public void handleConnection(VirtualSocket socket);

    public Map<String, String> getStats();
}
