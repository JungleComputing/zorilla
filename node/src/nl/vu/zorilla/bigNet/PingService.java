package nl.vu.zorilla.bigNet;

import java.io.IOException;

public interface PingService extends Service {
    
    public double ping(NodeInfo peer) throws IOException;

}