package ibis.zorilla.www;

import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;
import ibis.zorilla.cluster.Coordinates;
import ibis.zorilla.cluster.Neighbour;
import ibis.zorilla.job.Job;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.UUID;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import org.apache.log4j.Logger;
import org.mortbay.jetty.HttpException;

public final class PageServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger(PageServlet.class);

    private final Node node;

    PageServlet(WebService service, Node node) {
        this.node = node;
    }

    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        doGet(request, response);
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        try {
            response.setCharacterEncoding("UTF-8");
            response.setContentType("text/html");

            PrintWriter writer = response.getWriter();

            try {
                writePage(request.getPathInfo(), writer, false);
            } catch (HttpException e) {
                logger.debug("error in handling HTTP request", e);
                response.sendError(e.getStatus(), e.getReason());
            } catch (Exception e) {
                response.sendError(
                        HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e
                                .getMessage());
            }

            request.getInputStream().close();
        } catch (Exception e) {
            logger.warn("exception on handling servlet", e);
        }
    }

    public void writePage(String path, OutputStream out) throws Exception {
        PrintWriter writer = new PrintWriter(out);

        writePage(path, writer, true);

        writer.flush();
    }

    private void writePage(String path, PrintWriter writer, boolean remote)
            throws Exception {
        logger.debug("path = " + path);

        writeHeader(writer, remote);

        if (path == null || path.equalsIgnoreCase("/")) {
            writeOverviewPage(writer);
        } else if (path.equalsIgnoreCase("/jobs")) {
            writeJobsOverviewPage(writer);
        } else if (path.startsWith("/jobs/")) {
            writeJobDetailPage(writer, path.substring(6));
        } else if (path.equalsIgnoreCase("/cluster")) {
            writeClusterPage(writer);
        } else if (path.equalsIgnoreCase("/gossip")) {
            writeGossipPage(writer, remote);
        } else if (path.equalsIgnoreCase("/discovery")) {
            writeDiscoveryPage(writer);
        } else {
            throw new HttpException(HttpServletResponse.SC_NOT_FOUND,
                    "Cannot find page: " + path);
        }
        writeFooter(writer);
    }

    private void writeHeader(PrintWriter out, boolean remote) throws Exception {
        String linkPrefix = "";
        if (remote) {
            linkPrefix = WebService.linkTo(node.getInfo());
        }

        out
                .print("<html>"
                        + "<head>"
                        + "<link href=/resources/style.css type=text/css rel=StyleSheet>"
                        + "<title>Zorilla @ "
                        + node
                        + "</title>"
                        + "<link rel=\"shortcut icon\" href=/resources/favicon.ico />"
                        + "</head>"
                        + "<body>"
                        // main table
                        // + "<table width=100% cellspacing=10 rules=all
                        // frame=border>"
                        + "<table width=100% cellspacing=10>"
                        + "<tr>"
                        // ibis logo
                        + "<td align=center width=125px>"
                        + "<a href=http://www.cs.vu.nl/ibis><img src=/resources/ibis-logo.png></a>"
                        + "</td>"
                        // main title
                        + "<td valign=middle align=center>"
                        + "<h1>Zorilla Web Interface for "
                        + node
                        + "</h1>"
                        + "</td>"
                        // vu-kip
                        + "<td align=center>"
                        + "<a href=http://www.cs.vu.nl><img src=/resources/vu-kip.png></a>"
                        + "</td>"
                        // menu table
                        + "<tr><td id=menucell>"
                        + "<table class=menu>"
                        // links
                        + "<tr height=40px valign=center><td><a href=/>Home</a></td></tr>");

        if (remote) {
            String nodeName = node.getName();
            if (nodeName.length() > 15) {
                nodeName = nodeName.substring(0, 12) + "...";
            }
            out.print("<tr><td height=30px valign=bottom> @ " + nodeName
                    + "</td></tr>");
        } else {
            out.print("<tr><td height=30px valign=bottom></td></tr>");
        }

        out
                .print("<tr><td><a href="
                        + linkPrefix
                        + "/>Overview</a></td></tr>"
                        + "<tr><td><a href="
                        + linkPrefix
                        + "/jobs>Jobs</a></td></tr>"
                        + "<tr><td><a href="
                        + linkPrefix
                        + "/cluster>Cluster Service</a></td></tr>"
                        // + "<tr><td><a href=/vivaldi>Vivaldi
                        // Service</a></td></tr>"
                        + "<tr><td><a href="
                        + linkPrefix
                        + "/gossip>Gossip Service</a></td></tr>"
                        + "<tr><td><a href="
                        + linkPrefix
                        + "/discovery>Discovery Service</a></td></tr>"
                        // + "<tr><td><a href=/net>Network</a></td></tr>"
                        + "<tr><td><a href="
                        + linkPrefix
                        + "/log>Log</a></td></tr>"
                        // vl-e logo
                        + "<tr><td align=center class=menu height=200px valign=bottom>"
                        + "<a href=http://www.vl-e.nl><img src=/resources/vl-e.png alt=Vl-e</a>"
                        + "</td></tr>"
                        // asci logo
                        + "<tr><td align=center class=menu>"
                        + "<a href=http://www.asci.tudelft.nl><img src=/resources/asci.gif alt=ASCII</a>"
                        + "</td></tr>" + "</table>"
                        + "<td valign=top colspan=2>");
    }

    private void writeFooter(PrintWriter out) {
        out.println("</td></tr>");
        out.println("</table>");
        out.println("</body>");
        out.println("</html>");
    }

    private void printMap(Map<String, String> map, PrintWriter out) {
        out.println("<table>");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            out.println("<tr><td><strong>" + entry.getKey().replace('.', ' ')
                    + "</strong></td>");
            out.println("<td>" + entry.getValue() + "</td></tr>");
        }
        out.println("</table>");
    }

    private void printNodeList(NodeInfo[] nodes, PrintWriter out) throws Exception {
        if (nodes.length == 0) {
            out.println("- None -");
            return;
        }

        Coordinates nodeCoordinates = node.vivaldiService().getCoordinates();

        out.println("<table border=frame rules=all cellspacing=15>"
                + "<tr><th>Name</th>" + 
                // "<th>ID</th>" +
                // "<th>Address</th>" +
                "<th>Vivaldi Coordinate</th>" + "<th>Distance</th>");

        for (NodeInfo info : nodes) {
            out.println("<tr>");
            out.println("<td><a href=" + WebService.linkTo(info) + "/>"
                    + info.getName() + "</a></td>");
//            out.println("<td>" + info.getID() + "</td>");
            //out.println("<td>" + info.getAddress() + "</td>");
            out.println("<td>" + info.getCoordinates() + "</td>");
//            out.println("<td title=\"Coordinate: " + info.getCoordinates()
//                    + "\">");
            if (nodeCoordinates.isOrigin() || info.getCoordinates().isOrigin()) {
                out.println("<td>UNKNOWN");
            } else {
                out.println(String.format("<td align=center>%.2f", info.getCoordinates()
                        .distance(nodeCoordinates)));
            }

            out.println("</td></tr>");
        }

        out.println("</table>");

    }

    private void writeOverviewPage(PrintWriter out) {
        out.println("<h2>Overview</h2>");

        printMap(node.getStats(), out);
    }

    private void writeJobsOverviewPage(PrintWriter out) {
        out.println("<h2>Jobs</h2>");

        Job[] jobs = node.jobService().getJobs();

        if (jobs.length == 0) {
            out.println("<p>No jobs at this node</p>");
            writeFooter(out);
            return;
        }

        // header
        out.println("<table border=frame rules=all cellspacing=10>"
                + "<tr><th>ID</th>" + "<th>Phase</th>" + "<th>Primary</th>"
                + "<th>Workers</th>" + "<th>Local Workers</th>"
                + "<th>Native</th>" + "<th>Executable</th>");

        for (Job job : jobs) {
            Map<String, String> stats = job.getStats();
            out.println("<tr>");
            out.println("<td><a href=jobs/" + stats.get("ID") + ">"
                    + stats.get("ID") + "</a></td>");
            out.println("<td>" + stats.get("phase") + "</td>");
            out.println("<td>" + stats.get("primary") + "</td>");
            out.println("<td>" + stats.get("total.workers") + "</td>");
            out.println("<td>" + stats.get("local.workers") + "</td>");
            out.println("<td>" + stats.get("native") + "</td>");
            out.println("<td>" + stats.get("executable") + "</td>");
            out.println("</tr>");
        }

        out.println("</table>");
    }

    private void writeJobDetailPage(PrintWriter out, String jobID) {
        out.println("<h2>Job " + jobID + "</h2>");

        Job job;
        try {
            UUID uuid = UUID.fromString(jobID);
            job = node.jobService().getJob(uuid);
        } catch (IllegalArgumentException e) {
            out.println("Unknown Job");
            return;
        } catch (Exception e) {
            out.println("Unknown Job");
            return;
        }

        out.println("<h3>Attributes</h3>");
        printMap(job.getAttributes(), out);

        out.println("<h3>Stats</h3>");
        printMap(job.getStats(), out);
    }

    private void writeClusterPage(PrintWriter out) throws Exception {
        out.println("<h2>Cluster Service </h2>");

        printMap(node.clusterService().getStats(), out);
        printMap(node.vivaldiService().getStats(), out);

        out.println("<h3>Neighbours</h3>");

        Neighbour[] neighbours = node.clusterService().getSortedNeighbours();

        if (neighbours.length == 0) {
            out.println("- None -");
            return;
        }

        Coordinates nodeCoordinates = node.vivaldiService().getCoordinates();

        out.println("<table border=frame rules=all cellspacing=15>"
                + "<tr><th>Name</th>" 
                //+ "<th>ID</th>"
                + "<th>Vivaldi Coordinate</th>" + "<th>Vivaldi Distance</th>"
                + "<th>Actual Distance</th>");

        for (Neighbour neighbour : neighbours) {
            NodeInfo info = neighbour.getInfo();
            out.println("<tr>");
            out.println("<td><a href=" + WebService.linkTo(neighbour.getInfo()) + "/>"
                    + info.getName() + "</a></td>");
            //out.println("<td>" + info.getID() + "</td>");
            out.println("<td>" + info.getCoordinates() + "</td>");
            if (nodeCoordinates.isOrigin() || info.getCoordinates().isOrigin()) {
                out.println("<td>UNKNOWN</td>");
            } else {
                out.println(String.format("<td align=center>%.2f</td>", info
                        .getCoordinates().distance(nodeCoordinates)));
            }
            out.println(String.format("<td align=center>%.2f</td>", neighbour.distanceMs()));

            out.println("</tr>");
        }

        out.println("</table>");

    }

    private void writeGossipPage(PrintWriter out, boolean remote) throws Exception {
        String linkPrefix = "";
        if (remote) {
            linkPrefix = WebService.linkTo(node.getInfo());
        }

        out.println("<h2>Gossip Service </h2>");

        printMap(node.gossipService().getStats(), out);

        NodeInfo[] cache = node.gossipService().getNodesList();
        out.println("<h3>Gossip Cache</h3>");
        printNodeList(cache, out);

        NodeInfo[] fallbackCache = node.gossipService().getFallbackNodesList();
        out.println("<h3>Fallback Cache</h3>");
        printNodeList(fallbackCache, out);

        out.println("<h3>Statistics</h3>");
        out.println("<img src=" + linkPrefix + "/graphs/gossip/pns>");
        out.println("<img src=" + linkPrefix + "/graphs/gossip/exchanges>");
    }

    private void writeDiscoveryPage(PrintWriter out) throws Exception {
        out.println("<h2>Discovery Service </h2>");

        out.println("<h3>Nodes supplied by user</h3>");

        printMap(node.discoveryService().getStats(), out);

        NodeInfo[] nodes = node.discoveryService().getNodesList();
        printNodeList(nodes, out);

        out.println("<h3>Nodes Discovered by UDP broadcast</h3>");

        printMap(node.udpDiscoveryService().getStats(), out);

        NodeInfo[] udpNodes = node.udpDiscoveryService().getNodesList();
        printNodeList(udpNodes, out);
    }

}
