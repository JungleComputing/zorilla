package ibis.zorilla.www;

import ibis.util.IPUtils;
import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;
import ibis.zorilla.Service;

import java.io.DataInputStream;
import java.io.File;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.FileAppender;
import org.apache.log4j.HTMLLayout;
import org.apache.log4j.Logger;
import org.mortbay.jetty.HttpException;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.BoundedThreadPool;

import ibis.smartsockets.direct.DirectSocket;

public final class WebService implements Service {

    private static final Logger logger = Logger.getLogger(WebService.class);

    private final Server server;

    private final PageServlet pageServlet;

    private final LogServlet logServlet;

    private final GraphServlet graphServlet;

    private final ResourceServlet resourceServlet;
    
    static String linkTo(NodeInfo node) throws Exception {
    	URI uri = new URI(null, null, "/remote/" + node.getAddress() + "EOA", null);

    	
    	return uri.toASCIIString();
    }

    public WebService(Node node) throws Exception {
        System.setProperty("org.mortbay.log.class",
            "ibis.zorilla.www.JettyLogger");

        // save log to a temp file in HTML format
        File logFile = File.createTempFile("zorilla", ".log");
        logFile.deleteOnExit();
        FileAppender appender = new FileAppender(new HTMLLayout(), logFile
            .getAbsolutePath());
        Logger.getRootLogger().addAppender(appender);

        // Create the server
        server = new Server(node.config().getIntProperty(Config.WWW_PORT));
        BoundedThreadPool pool = new BoundedThreadPool();
        pool.setDaemon(true);
        server.setThreadPool(pool);

        // Create a context
        Context context = new Context(server, "/", Context.SESSIONS);
        
        pageServlet = new PageServlet(this, node);
        context.addServlet(new ServletHolder(pageServlet), "/*");

        logServlet = new LogServlet(logFile);
        context.addServlet(new ServletHolder(logServlet), "/log");

        graphServlet = new GraphServlet(node);
        context.addServlet(new ServletHolder(graphServlet), "/graphs/*");

        resourceServlet = new ResourceServlet();
        context.addServlet(new ServletHolder(resourceServlet), "/resources/*");
        context.addServlet(new ServletHolder(new RemoteServlet(node)),
            "/remote/*");

    }

    public void start() throws Exception {
        server.start();
        InetAddress address = IPUtils.getLocalHostAddress();
        
        logger.info("Webinterface available on " + "http://" + address.getHostAddress() +":"
            + server.getConnectors()[0].getLocalPort());
    }

    public void handleConnection(DirectSocket socket) {
        logger.debug("remote connection from " + socket.getRemoteAddress());
        
        try {
            OutputStream out = socket.getOutputStream();

            DataInputStream din = new DataInputStream(socket.getInputStream());

            String path = din.readUTF();


            if (path.equals("/log")) {
                logServlet.writeLog(out);
            } else if (path.startsWith("/resources/")) {
                resourceServlet.writeResource(path.substring(10), out);
            } else if (path.startsWith("/graphs/")) {
                graphServlet.writeGraph(path.substring(7), out);
            } else {
                pageServlet.writePage(path, out);
            }

            out.flush();
            socket.close();
        } catch (HttpException e) {
            logger.error("error on handling remote web connection", e);
            try {
                socket.close();
            } catch (Exception e2) {
                //IGNORE
            }
        } catch (Exception e) {
            logger.error("error on handling remote web connection", e);
        }
    }

    public synchronized Map<String, String> getStats() {
        Map<String, String> result = new HashMap<String, String>();
        return result;
    }

}
