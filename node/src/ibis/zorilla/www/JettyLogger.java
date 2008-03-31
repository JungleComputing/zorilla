package ibis.zorilla.www;

import org.apache.log4j.Logger;

public final class JettyLogger implements org.mortbay.log.Logger {

    private final Logger logger;

    public JettyLogger() {
        logger = Logger.getLogger(JettyLogger.class);
    }

    private JettyLogger(String name) {
        logger = Logger.getLogger(name);
    }

    public void debug(String message, Throwable exception) {
        logger.debug(message, exception);
    }

    public void debug(String message, Object arg1, Object arg2) {
        logger.debug(message + ":" + arg1 + " " + arg2);
    }

    public org.mortbay.log.Logger getLogger(String name) {
        return new JettyLogger(name);
    }

    public void info(String message, Object arg1, Object arg2) {
        logger.info(message + ":" + arg1 + " " + arg2);
    }

    public boolean isDebugEnabled() {
        return false;
    }

    public void setDebugEnabled(boolean arg0) {
        // IGNORE
    }

    public void warn(String message, Throwable exception) {
        logger.warn(message, exception);
    }

    public void warn(String message, Object arg1, Object arg2) {
        logger.warn(message + ":" + arg1 + " " + arg2);
    }

}
