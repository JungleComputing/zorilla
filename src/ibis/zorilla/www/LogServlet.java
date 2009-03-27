package ibis.zorilla.www;

import java.io.File;
import java.io.FileInputStream;
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
 * Serve the log file
 */
public final class LogServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger(ResourceServlet.class);

    private final File file;

    LogServlet(File file) {
        this.file = file;
    }

    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        request.setCharacterEncoding("UTF-8");
        response.setContentType("text/html");

        OutputStream out = response.getOutputStream();
        writeLog(out);
        request.getInputStream().close();
    }
    
    public void writeLog(OutputStream out) throws IOException {
        InputStream input = new FileInputStream(file);
        byte[] bytes = new byte[1024];
        while (true) {
            int read = input.read(bytes);

            if (read == -1) {
                out.close();
                return;
            }
            out.write(bytes, 0, read);
        }
    }

    public String getServletInfo() {
        return "Log Servlet";
    }

    public synchronized void destroy() {
        logger.debug("Destroyed");
    }

}
