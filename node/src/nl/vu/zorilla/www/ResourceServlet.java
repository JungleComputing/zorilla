package nl.vu.zorilla.www;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

/**
 * Serves (semi) static content from a file or resource
 */
public final class ResourceServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger(ResourceServlet.class);

    ResourceServlet() {
        // NOTHING
    }

    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        String path = request.getPathInfo();

        logger.debug("loading resource: " + path);

        InputStream input = ClassLoader.getSystemResourceAsStream("resources"
                + path);

        if (input == null) {
            response.sendError(404);
            return;
        }

        OutputStream out = response.getOutputStream();
        byte[] bytes = new byte[1024];
        while (true) {
            int read = input.read(bytes);

            if (read == -1) {
                out.close();
                request.getInputStream().close();
                return;
            }
            out.write(bytes, 0, read);
        }
    }
    
    public void writeResource(String path, OutputStream out) throws IOException {

        logger.debug("loading resource: " + path);

        InputStream input = ClassLoader.getSystemResourceAsStream("resources"
                + path);

        if (input == null) {
            return;
        }

        byte[] bytes = new byte[1024];
        while (true) {
            int read = input.read(bytes);

            if (read == -1) {
                return;
            }
            out.write(bytes, 0, read);
        }
    }

    public String getServletInfo() {
        return "Resource Servlet";
    }

    public synchronized void destroy() {
        logger.debug("Destroyed");
    }

}
