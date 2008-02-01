package nl.vu.zorilla.ui;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import nl.vu.zorilla.Job;
import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaException;
import nl.vu.zorilla.zoni.JobInfo;
import nl.vu.zorilla.zoni.ZoniException;
import nl.vu.zorilla.zoni.ZoniProtocol;

import org.apache.log4j.Logger;
import org.gridlab.gat.URI;

public class ZoniServer implements Runnable {

    private final Logger logger = Logger.getLogger(ZoniServer.class);

    private final ServerSocket serverSocket;

    private ArrayList<ServerConnection> connections = new ArrayList<ServerConnection>();
    
    private final Node node;
    
    File magicFile;
    byte[] magicData;

    public ZoniServer(Node node, int port) throws IOException {
        this.node = node;
        
        InetAddress localHost = InetAddress.getByName(null);
        
        serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress(localHost, port));
        
        new Thread(this).start();
    }
    
    public final int getPort() {
        return serverSocket.getLocalPort();
    }

    public void stop() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.debug("error on closing", e);
        }

        synchronized (this) {
            for (ServerConnection connection: connections) {
                connection.close();
            }
            connections.clear();
        }

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
