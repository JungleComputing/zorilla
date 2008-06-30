package zolo.strategy;

import org.apache.commons.configuration.*;
import java.util.Iterator;
import java.util.List;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;


public class TimeIntervalStrategy extends Strategy implements StrategyInterface {

    /**
     * Run this strategy
     */
    public void run() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimeIntervalTask(), 0, Integer.parseInt(config.getString("timer")) * 1000);
    }

    private class TimeIntervalTask extends TimerTask {
        public void run() {
            check();
        }
    }

    /**
     * Check if conditions are met
     */
    public void check() {
        // Get all time intervals
        List intervals = config.configurationsAt("intervals.interval");

        // Get current time in milliseconds
        Calendar today = Calendar.getInstance();
        Long currentTimeMillis = today.getTimeInMillis();

        // Check all intervals
        for (Iterator it = intervals.iterator(); it.hasNext();) {
            HierarchicalConfiguration interval = (HierarchicalConfiguration) it.next();

            // Create interval start time in millis
            Calendar intervalStartTime = Calendar.getInstance();
            intervalStartTime.set(Calendar.HOUR_OF_DAY, Integer.parseInt(interval.getString("start-hour")));
            intervalStartTime.set(Calendar.MINUTE, Integer.parseInt(interval.getString("start-minute")));
            Long intervalStartMillis = intervalStartTime.getTimeInMillis();

            // Create interval end time in millis
            Calendar intervalEndTime = Calendar.getInstance();
            intervalEndTime.set(Calendar.HOUR_OF_DAY, Integer.parseInt(interval.getString("end-hour")));
            intervalEndTime.set(Calendar.MINUTE, Integer.parseInt(interval.getString("end-minute")));
            Long intervalEndMillis = intervalEndTime.getTimeInMillis();

            // Run when current time is in interval
            if (currentTimeMillis >= intervalStartMillis &&
               (currentTimeMillis < intervalEndMillis || intervalEndMillis < intervalStartMillis)) {
                manager.reportState(this, StrategyState.GO);
                return;
            }
        }

        // Do not run
        manager.reportState(this, StrategyState.NO_GO);
    }
}
