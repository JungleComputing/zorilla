package ibis.zorilla.starter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class Starter {

    private static final int INTERVAL = 60 * 1000; // 1 minute

    private static final int BUFFER_SIZE = 32 * 1024;

    private static final int TIMEOUT = 10 * 1000; // 10 seconds
    
    private final URL location;

    private final File workingDir;

    private final File zipFile;

    private final File libDir;

    private long timestamp;

    Starter(URL location, File workingDir) {
        this.location = location;
        this.workingDir = workingDir;
        timestamp = 0;

        zipFile = new File(workingDir, "zorilla-libs.zip");

        libDir = new File(workingDir, "lib");

    }

    private long getTimestamp() {
        URLConnection connection;
        try {
            connection = location.openConnection();
            connection.setConnectTimeout(TIMEOUT);
            connection.setReadTimeout(TIMEOUT);
            long result = connection.getLastModified();

            return result;
        } catch (IOException e) {
            System.err.println("could not get timestamp");
            e.printStackTrace(System.err);
            return 0;
        }

    }

    private void delete(File file) {
        if (file.isFile()) {
            file.delete();
        } else if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                delete(child);
            }
            file.delete();
        }
    }

    private void download() throws IOException {
        delete(zipFile);

        URLConnection connection;
        try {
            connection = location.openConnection();
            
            connection.setConnectTimeout(TIMEOUT);
            connection.setReadTimeout(TIMEOUT);
            
            InputStream in = connection.getInputStream();

            FileOutputStream out = new FileOutputStream(zipFile);

            System.err.println("STARTER: dowloading "
                    + connection.getContentLength() + " bytes from " + location);

            byte[] buffer = new byte[BUFFER_SIZE];
            boolean done = false;
            while (!done) {
                int read = in.read(buffer);

                if (read == -1) {
                    done = true;
                } else {
                    out.write(buffer, 0, read);
                }
            }
            in.close();
            out.close();

            timestamp = connection.getLastModified();
        } catch (IOException e) {
            timestamp = 0;
            throw e;
        }
    }

    private void unzip() throws IOException {
        delete(libDir);

        System.err.println("STARTER: unzipping files");

        libDir.mkdirs();

        ZipFile zipFile = new ZipFile(this.zipFile);

        Enumeration<? extends ZipEntry> entries = zipFile.entries();

        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();

            File file = new File(libDir, entry.getName());

            if (entry.isDirectory()) {
                file.mkdirs();
            } else {
                // create parent directory
                File parent = file.getParentFile();
                if (parent != null) {
                    parent.mkdirs();
                }

                // unzip file to destination file
                FileOutputStream out = new FileOutputStream(file);
                InputStream in = zipFile.getInputStream(entry);

                byte[] buffer = new byte[BUFFER_SIZE];
                boolean done = false;
                while (!done) {
                    int read = in.read(buffer);

                    if (read == -1) {
                        done = true;
                    } else {
                        out.write(buffer, 0, read);
                    }
                }
                in.close();
                out.close();
            }
        }

    }

    private String buildClasspath(File file) {
        if (file.isFile()) {
            return file.getAbsolutePath();
        } else if (file.isDirectory()) {
            String result = file.getAbsolutePath();
            for (File child : file.listFiles()) {
                result = result + ":" + buildClasspath(child);
            }
            return result;
        } else {
            return "";
        }

    }

    private Process startProcess() throws IOException {
        ProcessBuilder builder = new ProcessBuilder();

        String javaExecutable =
            System.getProperty("java.home") + File.separator + "bin"
                    + File.separator + "java";

        builder.command().add(javaExecutable);
        builder.command().add("-server");

        builder.command().add("-cp");
        builder.command().add(buildClasspath(libDir));

        // log4j stuff
        builder.command().add(
            "-Dlog4j.configuration=file:" + libDir.getAbsolutePath()
                    + File.separator + "log4j.properties");

        //add user home (just in case)
        builder.command().add("-Duser.home=" + System.getProperty("user.home"));
        
        builder.command().add("ibis.zorilla.Main");

        System.err.print("STARTER: starting process: ");
        for (String string : builder.command()) {
            System.err.print(string + " ");
        }
        System.err.println();

        Process result = builder.start();

        // forward stdout and stderr of process.

        StreamReader outReader =
            new StreamReader(result.getInputStream(), System.out);
        outReader.setDaemon(true);
        outReader.start();

        StreamReader errReader =
            new StreamReader(result.getErrorStream(), System.err);
        errReader.setDaemon(true);
        errReader.start();

        return result;
    }

    private void run() {
        Process process = null;

        while (true) {
            //check if process has ended
            if (process != null) {
                try {
                    process.exitValue();
                    process = null;
                } catch (IllegalThreadStateException e) {
                    // Process still running, ignore
                }
            }

            // check timestamp, if newer, kill the process
            if (process != null && (timestamp != getTimestamp())) {
                System.err.println("STARTER: destroying process");
                process.destroy();
                process = null;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // IGNORE
                }
            }

            if (process == null) {
                try {
                    workingDir.mkdirs();
                    download();
                    unzip();

                    process = startProcess();
                } catch (Throwable e) {
                    System.err.println("STARTER: could not start process");
                    e.printStackTrace(System.err);
                }
            }

            // wait a while
            try {
                Thread.sleep(INTERVAL);
            } catch (InterruptedException e) {
                // IGNORE
            }
        }

    }

    public static void main(String[] args) {
        String location = "http://www.cs.vu.nl/~ndrost/zorilla-lib.zip";
        String workingDir =
            System.getProperty("java.io.tmpdir") + File.separator
                    + "zorilla-starter";

        System.err.println("system properties: ");
        System.getProperties().list(System.err);
        
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("--dir")) {
                i++;
                workingDir = args[i];
            } else {
                location = args[i];
            }
        }

        try {
            Starter starter =
                new Starter(new URL(location), new File(workingDir));

            starter.run();
        } catch (MalformedURLException e) {
            System.err.println("STARTER: cannot create url from location: "
                    + location);
            e.printStackTrace();
        }

    }
}
