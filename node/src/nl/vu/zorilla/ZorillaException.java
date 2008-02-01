package nl.vu.zorilla;

/**
 * Like java.lang.Exception, but with a cause.
 */
public class ZorillaException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs an <code>ZorillaException</code> with <code>null</code> as
     * its error detail message.
     */
    public ZorillaException() {
        super();
    }

    /**
     * Constructs an <code>ZorillaException</code> with the specified detail
     * message.
     * 
     * @param s
     *            the detail message
     */
    public ZorillaException(String s) {
        super(s);
    }

    /**
     * Constructs an <code>ZorillaException</code> with the specified detail
     * message and cause.
     * 
     * @param s
     *            the detail message
     * @param cause
     *            the cause
     */
    public ZorillaException(String s, Throwable cause) {
        super(s, cause);
    }

    /**
     * Constructs an <code>ZorillaException</code> with the specified cause.
     * 
     * @param cause
     *            the cause
     */
    public ZorillaException(Throwable cause) {
        super(cause);
    }
}
