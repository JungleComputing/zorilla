package ibis.zorilla.net;

import ibis.util.ThreadPool;
import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.Service;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import ibis.smartsockets.direct.DirectSocket;

public class ZoniService implements Service, Runnable {

    private final Logger logger = Logger.getLogger(ZoniService.class);

    private final ServerSocket serverSocket;

    private ArrayList<ZoniServiceConnection> connections = new ArrayList<ZoniServiceConnection>();

    private final Node node;

    File magicFile;

    byte[] magicData;

    public ZoniService(Node node) throws IOException {
        this.node = node;

        int port = node.config().getIntProperty(Config.ZONI_PORT);

        if (port == -1) {
            serverSocket = null;
        } else {
            serverSocket = new ServerSocket(port);
        }

        // InetAddress localHost = InetAddress.getByName(null);
        // serverSocket.bind(new InetSocketAddress(localHost, port));

    }

    public void start() {
        if (serverSocket == null) {
            logger.info("Zoni Service disabled");
        } else {
            ThreadPool.createNew(this, "Zoni Service");

            logger.info("Started Zoni service on port "
                    + serverSocket.getLocalPort());
        }
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

                ZoniServiceConnection connection = new ZoniServiceConnection(
                        socket, node);

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
