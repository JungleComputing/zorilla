package nl.vu.zorilla.www;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.UnknownHostException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.net.Network;

import org.apache.log4j.Logger;

import smartsockets.direct.DirectSocket;
import smartsockets.direct.DirectSocketAddress;

/**
 * Serves (semi) static content from a file or resource
 */
public final class RemoteServlet extends HttpServlet {

    // short connection timeout, as this is interactive
    public static final int CONNECT_TIMEOUT = 1000;

    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger(RemoteServlet.class);

    private final Node node;

    RemoteServlet(Node node) {
        this.node = node;
    }

    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        DirectSocketAddress address;

        String[] strings = request.getPathInfo().substring(1).split("EOA");
        if (strings.length != 2) {
        	throw new IOException("illegal address: " + request.getPathInfo());
        }
        
        String addressString = strings[0];
        String path = strings[1];

        // get the address we need to connect to
        try {
            address = DirectSocketAddress.getByAddress(addressString);
        } catch (UnknownHostException e) {
            response.sendError(404, "Illegal node addess");
            return;
        }

        try {
            logger.debug("connecting to remote node");
            DirectSocket connection = node.network().connect(address,
                Network.WEB_SERVICE, CONNECT_TIMEOUT);

            logger.debug("sending out request");

            DataOutputStream out = new DataOutputStream(connection
                .getOutputStream());
            out.writeUTF(path);
            out.flush();

            logger.debug("piping result to user");
            InputStream in = connection.getInputStream();
            OutputStream rout = response.getOutputStream();
            byte[] bytes = new byte[1024];
            boolean empty = true;
            while (true) {
                int read = in.read(bytes);

                if (read == -1) {
                    if (empty) {
                        //we did not receive anything
                        response.sendError(500, "Could not fetch page at "
                            + address);
                    }
                    rout.close();
                    logger.debug("done");
                    return;
                } else if (read > 0) {
                    empty = false;
                }
                rout.write(bytes, 0, read);
            }

        } catch (Exception e) {
            logger.warn("could not forward request to " + address);
            response.sendError(500, "Could not contact node at " + address);
        }

        request.getInputStream().close();
    }

    public String getServletInfo() {
        return "Resource Servlet";
    }

    public synchronized void destroy() {
        logger.debug("Destroyed");
    }

}
