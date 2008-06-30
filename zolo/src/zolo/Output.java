package zolo;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;


public class Output {

    private static boolean verboseOutput = false;

    /*
     * Constructor
     */
    public Output() {
    }

    /*
     * Set verbosity on / off
     *
     * @param   verboseOutput   Do we want verbose output?
     */
    public void setVerbosity(boolean verboseOutput) {
        this.verboseOutput = verboseOutput;
    }

    /*
     * Display verbose output (if set)
     *
     * @param   output   String to display
     * @param   eol      Display end of line after string
     */
    public void verbose(String output, boolean eol) {
        // Add current time to output
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        Date date = new java.util.Date();
        output = "[" + dateFormat.format(date) + "] " + output;
        if (verboseOutput == true) {
            if(eol) {
                System.out.println(output);
            } else {
                System.out.print(output);
            }
        }
    }

    /*
     * Display Exception
     *
     * @param   e      Exception to display
     * @param   exit   Exit after display
     */
    public void error(Exception e, boolean exit) {
        System.out.println("");
        System.out.println("************************************");
        System.out.println("EXCEPTION:");
        System.out.println(e.getMessage());
        System.out.println("");
        e.printStackTrace();
        System.out.println("************************************");
        System.out.println("");

        if (exit) {
            System.exit(1);
        }
    }
}
