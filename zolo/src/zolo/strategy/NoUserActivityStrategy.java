package zolo.strategy;

import zolo.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.sun.jna.*;
import com.sun.jna.win32.*;
import java.util.Timer;
import java.util.TimerTask;


public class NoUserActivityStrategy extends Strategy implements StrategyInterface {

    public interface Kernel32 extends StdCallLibrary {
        // Load kernel32 library
        Kernel32 INSTANCE = (Kernel32)Native.loadLibrary("kernel32", Kernel32.class);

        /**
         * Retrieves the number of milliseconds that have elapsed since the system was started
         * http://msdn2.microsoft.com/en-us/library/ms724408.aspx
         *
         * @return Number of milliseconds that have elapsed since the system was started.
         */
        public int GetTickCount();
    };

    public interface User32 extends StdCallLibrary {
        // Load user32 library
        User32 INSTANCE = (User32)Native.loadLibrary("user32", User32.class);

        /**
         * Contains the time of the last input
         * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/winui/winui/windowsuserinterface/userinput/keyboardinput/keyboardinputreference/keyboardinputstructures/lastinputinfo.asp
         */
        public static class LASTINPUTINFO extends Structure {
            public int cbSize = 8;

            /// Tick count of when the last input event was received.
            public int dwTime;
        }

        /**
         * Retrieves the time of the last input event
         * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/winui/winui/windowsuserinterface/userinput/keyboardinput/keyboardinputreference/keyboardinputfunctions/getlastinputinfo.asp
         *
         * @return   Time of the last input event, in milliseconds
         */
        public boolean GetLastInputInfo(LASTINPUTINFO result);
    };

    /**
     * Get the amount of milliseconds that have elapsed since the last input event
     * (mouse or keyboard)
     *
     * @return   Idle time in milliseconds
     */
    public static int getIdleTimeMillisWin32() {
        User32.LASTINPUTINFO lastInputInfo = new User32.LASTINPUTINFO();
        User32.INSTANCE.GetLastInputInfo(lastInputInfo);
        return Kernel32.INSTANCE.GetTickCount() - lastInputInfo.dwTime;
    }

    /**
     * Run this strategy
     */
    public void run() throws ZoloException {
        // Check OS
        if (!manager.getOS().equals("Windows")) {
            manager.reportState(this, StrategyState.ERROR);
        }

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new NoUserActivityTask(), 0, Integer.parseInt(config.getString("timer")) * 1000);
    }


    private class NoUserActivityTask extends TimerTask {
        public void run() {
            check();
        }
    }


    /**
     * Check
     */
    public void check() {

        int idleSec = getIdleTimeMillisWin32() / 1000;

        // Report status based on Windows idle time
        if (idleSec >= Integer.parseInt(config.getString("user-idle-after"))) {
            manager.reportState(this, StrategyState.GO);
        } else {
            manager.reportState(this, StrategyState.NO_GO);
        }
    }
}
