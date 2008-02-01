package ibis.zorilla.www;

import ibis.zorilla.Node;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * Serves (semi) static content from a file or resource
 */
public final class GraphServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    Node node;

    GraphServlet(Node node) {
        this.node = node;
    }

    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        String path = request.getPathInfo();

        try {
            if (path.startsWith("/gossip/")) {
                node.gossipService().writeGraph(path.substring(8),
                        response.getOutputStream());
            } else {
                response.sendError(404, "Could not find graph " + path);
            }
        } catch (Exception e) {
            response.sendError(500, e.getMessage());
        }
    }

    public void writeGraph(String path, OutputStream out) {
        if (path.startsWith("/gossip/")) {
            try {
                node.gossipService().writeGraph(path.substring(8), out);
            } catch (Exception e) {
                return;
            }
        }
    }

}
