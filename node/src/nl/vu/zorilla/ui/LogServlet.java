package nl.vu.zorilla.ui;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.mortbay.http.HttpException;

public final class LogServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger(LogServlet.class);

    String pageType;
    
    WebInterface webInterface;

    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        
        webInterface = WebInterface.getInstance();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        File log;
        
        request.setAttribute("Log", this);
        getServletContext().setAttribute("Log", this);

        String lengthString = request.getParameter("length");
        if (lengthString != null && lengthString.length() > 0) {
            response.setContentLength(Integer.parseInt(lengthString));
        }

        String buffer = request.getParameter("buffer");
        if (buffer != null && buffer.length() > 0) {
            response.setBufferSize(Integer.parseInt(buffer));
        }

        request.setCharacterEncoding("UTF-8");
        response.setContentType("text/html");

        String pi = request.getPathInfo();
        if (pi == null) {
            throw new HttpException(501);
        } else if (pi.equals("/debug")) {
            log = webInterface.debugFile();
        } else if (pi.equals("/info")) {
            log = webInterface.infoFile();
        } else {
            return;
            //throw new HttpException(404);
        }
            
        OutputStream out = response.getOutputStream();
        
        byte[] bytes = new byte[1024];
        
        FileInputStream input = new FileInputStream(log);
        
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

    public String getServletInfo() {
        return "Dump Servlet";
    }

    public synchronized void destroy() {
        logger.debug("Destroyed");
    }

}
