package nl.vu.zorilla.bigNet;

import java.net.DatagramPacket;

import smartsockets.direct.DirectSocket;

public interface Service {
    
    public void start();
    
    public void handleMessage(DatagramPacket packet);
    
    public void handleConnection(DirectSocket socket);

}
