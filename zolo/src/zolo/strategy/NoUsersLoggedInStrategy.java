package zolo.strategy;

import java.util.Timer;
import java.util.TimerTask;
import java.util.*;
import java.io.*;
import zolo.*;
import zolo.starter.*;


public class NoUsersLoggedInStrategy extends Strategy implements StrategyInterface {

    /**
     * Run the strategy
     */
    public void run() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new NoUsersLoggedInTask(), 0, Integer.parseInt(config.getString("timer")) * 1000);
    }

    /**
     * TimerTask
     */
    private class NoUsersLoggedInTask extends TimerTask {
        public void run() {
            check();
        }
    }

    /**
     * Check if user is logged in
     */
    public void check() {
       try {
            String userProc = manager.getOS().equals("Linux") ? "users" : "../../external/psloggedon.exe";

            // See: http://mindprod.com/jgloss/exec.html
            ProcessBuilder pb = new ProcessBuilder(userProc);
            pb.redirectErrorStream( true );

            final Process p = pb.start();
            new Thread() {
                @SuppressWarnings( { "EmptyCatchBlock" } )
                public void run() {
                    final InputStream is = p.getInputStream();
                    final InputStreamReader isr = new InputStreamReader(is);
                    final BufferedReader br = new BufferedReader(isr, 100);
                    String line;
                    String prevline = new String("");
                    try {
                        try {
                            if (manager.getOS().equals("Linux")) {
                                if ((line = br.readLine()) == null) {
                                    manager.reportState(strategyObj, StrategyState.GO);
                                } else {
                                    manager.reportState(strategyObj, StrategyState.NO_GO);
                                    while (br.readLine() != null) { }
                                }
                            } else { // Windows
                                if (br.readLine() == null) {
                                    manager.reportState(strategyObj, StrategyState.ERROR);
                                }
                                while ((line = br.readLine()) != null) {
                                    if (prevline.equals("NT AUTHORITY\\NETWORK SERVICE") &&
                                           line.equals("     Error: could not retrieve logon time")) {
                                        manager.reportState(strategyObj, StrategyState.GO);
                                        return;
                                    }
                                    prevline = line;
                                }
                                manager.reportState(strategyObj, StrategyState.NO_GO);
                            }
                        } catch (EOFException e) { }
                        br.close();
                    } catch (IOException e) { }
                }
            }.start();

            // Spawn thread to write input to the spawned program
            new Thread() {
                public void run() {
                    final OutputStream os = p.getOutputStream();
                    final OutputStreamWriter osw = new OutputStreamWriter(os);
                    final BufferedWriter bw = new BufferedWriter(osw, 100);
                    String line;
                    try {
                        bw.write("some text\n" );
                        bw.write("some more text\n" );
                        bw.close();
                    } catch (IOException e) { }
                }
            }.start();

            try {
                // wait for spawned program to terminate.
                p.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Close streams
            p.getInputStream().close();
            p.getOutputStream().close();
            p.getErrorStream().close();
        } catch (Exception e) {
            out.error(e, true);
        }
    }
}
