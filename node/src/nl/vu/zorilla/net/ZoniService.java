package nl.vu.zorilla.net;

import ibis.util.ThreadPool;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import nl.vu.zorilla.Config;
import nl.vu.zorilla.Node;
import nl.vu.zorilla.Service;

import org.apache.log4j.Logger;

import smartsockets.direct.DirectSocket;

public class ZoniService implements Service, Runnable {

    private final Logger logger = Logger.getLogger(ZoniService.class);

    private final ServerSocket serverSocket;

    private ArrayList<ServerConnection> connections = new ArrayList<ServerConnection>();

    private final Node node;

    File magicFile;

    byte[] magicData;

    public ZoniService(Node node) throws IOException {
        this.node = node;

        InetAddress localHost = InetAddress.getByName(null);

        serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress(localHost, node.config()
                .getIntProperty(Config.ZONI_PORT)));
    }

    public void start() {
        ThreadPool.createNew(this, "Zoni Service");

        logger.info("Started Zoni service on port "
                + serverSocket.getLocalPort());
    }
    
    public synchronized Map<String, String> getStats() {
        Map<String, String> result = new HashMap<String, String>();

        return result;
    }
    
    public void handleConnection(DirectSocket socket) {
        logger.error("TCP connection to UDP discovery service");
    }

    public final void run() {
        try {

            while (!serverSocket.isClosed()) {

                Socket socket = serverSocket.accept();

                ServerConnection connection = new ServerConnection(socket, node);

                synchronized (this) {
                    connections.add(connection);
                }

            }

        } catch (IOException e) {
            // INGORE
        }
        logger.debug("server exits");
    }

}
