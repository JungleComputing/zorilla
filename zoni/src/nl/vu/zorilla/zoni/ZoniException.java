package nl.vu.zorilla.zoni;

/**
 * Like java.lang.Exception.
 */
public class ZoniException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs an <code>ZorillaException</code> with <code>null</code> as
     * its error detail message.
     */
    public ZoniException() {
        super();
    }

    /**
     * Constructs an <code>ZorillaException</code> with the specified detail
     * message.
     * 
     * @param s
     *            the detail message
     */
    public ZoniException(String s) {
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
    public ZoniException(String s, Throwable cause) {
        super(s, cause);
    }

    /**
     * Constructs an <code>ZorillaException</code> with the specified cause.
     * 
     * @param cause
     *            the cause
     */
    public ZoniException(Throwable cause) {
        super(cause);
    }
}
