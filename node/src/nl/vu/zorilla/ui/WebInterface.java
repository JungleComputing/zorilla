package nl.vu.zorilla.ui;

import java.io.File;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaError;

import org.apache.log4j.FileAppender;
import org.apache.log4j.HTMLLayout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mortbay.http.HttpContext;
import org.mortbay.http.HttpServer;
import org.mortbay.http.SocketListener;
import org.mortbay.http.handler.NotFoundHandler;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.util.MultiException;

public final class WebInterface {

    private static WebInterface instance;

    static synchronized WebInterface getInstance() {
        if (instance == null) {
            throw new ZorillaError(
                "tried to get webinterface instance before it was set");
        }
        return instance;
    }

    static synchronized void setInstance(WebInterface webInterface) {
        instance = webInterface;
    }

    private final HttpServer server;
    
    private final int port;

    private final Node node;

    private final File infoFile;

    private final File debugFile;

    public WebInterface(Node node) throws Exception {
        this.node = node;

        try {

        // infofile
        
        infoFile = File.createTempFile("zorilla", ".log");
        
        infoFile.deleteOnExit();


        FileAppender infoAppender = new FileAppender(new HTMLLayout(), infoFile
            .getAbsolutePath());
        infoAppender.setThreshold(Level.INFO);

        Logger.getRootLogger().addAppender(infoAppender);

        // debugfile

        debugFile = File.createTempFile("zorilla", ".log");
        debugFile.deleteOnExit();
        
        FileAppender debugAppender = new FileAppender(new HTMLLayout(), debugFile
            .getAbsolutePath());

        Logger.getRootLogger().addAppender(debugAppender);

        setInstance(this);

        // Create the server
        server = new HttpServer();

        // Create a port listener
        SocketListener listener = new SocketListener();
        listener.setPort(node.config().getWwwPort());
        server.addListener(listener);

        // Create a context
        HttpContext context = new HttpContext();
        context.setContextPath("/");
        server.addContext(context);

        // Create a servlet container
        ServletHandler servlets = new ServletHandler();
        context.addHandler(servlets);

        // Map a servlet onto the container
        servlets.addServlet("Log", "/logs/*", "nl.vu.zorilla.ui.LogServlet");
        servlets.addServlet("Dump", "/dump/*", "nl.vu.zorilla.ui.DumpServlet");
        servlets.addServlet("Node", "/", "nl.vu.zorilla.ui.NodeServlet");

        context.addHandler(new NotFoundHandler());

        server.start();
        port = listener.getPort();
        
        
        
        } catch (MultiException e) {
            if (e.size() == 1) {
                throw e.getException(0);
            }
            throw e;
        }
    }

    public void stop() {
        try {
            server.stop();
            server.destroy();
        } catch (Exception e) {
            // IGNORE
        }
    }
    
    File debugFile() {
        return debugFile;
    }

    File infoFile() {
        return infoFile;
    }

    Node node() {
        return node;
    }

    public int getPort() {
       return port;
    }

}
