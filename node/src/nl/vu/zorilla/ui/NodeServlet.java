package nl.vu.zorilla.ui;

import java.io.IOException;
import java.io.PrintWriter;

import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.vu.zorilla.Job;
import nl.vu.zorilla.Node;

import org.apache.log4j.Logger;
import org.mortbay.html.Break;
import org.mortbay.html.Font;
import org.mortbay.html.Heading;
import org.mortbay.html.Link;
import org.mortbay.html.Page;
import org.mortbay.html.Table;

public final class NodeServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger(NodeServlet.class);

    String pageType;

    WebInterface webInterface;

    Node node;

    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        webInterface = WebInterface.getInstance();
        node = webInterface.node();
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        doGet(request, response);
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        request.setAttribute("Node", this);
        getServletContext().setAttribute("Node", this);

        String length = request.getParameter("length");
        if (length != null && length.length() > 0) {
            response.setContentLength(Integer.parseInt(length));
        }

        String buffer = request.getParameter("buffer");
        if (buffer != null && buffer.length() > 0)
            response.setBufferSize(Integer.parseInt(buffer));

        request.setCharacterEncoding("UTF-8");
        response.setContentType("text/html");

        PrintWriter pout = response.getWriter();
        Page page = null;

        try {
            page = new Page();

            page.add(new Heading(1, "Zorilla Node"));

            page.title("Zorilla Node " + node);

            Map<String, Object> info = node.getInfo();

            if (info != null) {

                Table table = new Table(0).cellPadding(0).cellSpacing(0);
                page.add(table);

                for (Map.Entry<String, Object> infoLine : info.entrySet()) {

                    table.newRow();
                    table.addHeading(infoLine.getKey() + ":&nbsp;").cell()
                            .left();
                    table.addCell(infoLine.getValue().toString());

                }

            }

            /*            *//** ***** NEIGHBOURS ***** */
            /*
             * 
             * Neighbour[] neighbours = node.network().getNeighbours();
             * 
             * if (neighbours.length > 0) {
             * 
             * page.add(new Heading(2, "Neighbourhood"));
             * 
             * Table table = new Table(0).cellPadding(0).cellSpacing(0);
             * page.add(table);
             * 
             * table.newRow(); table.addHeading("Host"); table.addHeading("Round
             * trip time");
             * 
             * for (Neighbour neighbour : neighbours) { table.newRow();
             * 
             * String link = "http://" +
             * neighbour.address().inetAddress().getHostAddress() + ":" +
             * node.config().getWwwPort(); table.addCell(new Link(link,
             * neighbour.address() .inetAddress().getHostName()));
             * table.addCell(neighbour.latency() + " ms").cell().center(); } }
             */
            /** ***** JOBS ****** */

            Job[] jobs = node.getJobs();

            if (jobs.length > 0) {

                page.add(new Heading(2, "Jobs"));

                Table table = new Table(0).cellPadding(0).cellSpacing(0);
                page.add(table);

                for (int i = 0; i < jobs.length; i++) {
                    table.newRow();
                    table.newHeading().cell().nest(new Font(1, true)).add(
                            "<BR>Job " + (i + 1) + " of " + jobs.length
                                    + "<BR>").attribute("COLSPAN", "2").left();

                    table.newRow();
                    table.addHeading("Job ID:&nbsp;").cell().left();
                    table.addCell(jobs[i].getID());

                    Map<String, String> attributes = jobs[i].getAttributes();

                    String attributesString = "";

                    for (Map.Entry<String, String> entry : attributes
                            .entrySet()) {
                        attributesString += entry.getKey() + " = "
                                + entry.getValue() + "<br>";
                    }

                    table.newRow();
                    table.addHeading("Attributes:&nbsp;").cell().left();
                    table.addCell(attributesString);

                    Map<String, String> status = jobs[i].getStatus();

                    String statusString = "";

                    for (Map.Entry<String, String> entry : status.entrySet()) {
                        statusString += entry.getKey() + " = "
                                + entry.getValue() + "<br>";
                    }

                    table.newRow();
                    table.addHeading("Status:&nbsp;").cell().left();
                    table.addCell(statusString);

                }

            }

            page.add(new Heading(2, "Logs"));

            page.add(new Link("logs/info", "Info level log"));
            page.add(Break.para);
            page.add(new Link("logs/debug", "Debug level log"));
            page.add(Break.para);

        } catch (Exception e) {
            logger.warn("exception on handling servlet", e);
        }

        page.write(pout);

        pout.close();

        request.getInputStream().close();

    }

    public String getServletInfo() {
        return "Dump Servlet";
    }

    public synchronized void destroy() {
        logger.debug("Destroyed");
    }
}
