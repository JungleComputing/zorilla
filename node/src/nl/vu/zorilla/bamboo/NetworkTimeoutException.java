package nl.vu.zorilla.bamboo;



final class NetworkTimeoutException extends NetworkException {

    private static final long serialVersionUID = 1L;

    public NetworkTimeoutException() {
        super();
    }

    /**
     * Constructs an <code>ZorillaException</code> with the specified detail
     * message.
     * 
     * @param s
     *            the detail message
     */
    public NetworkTimeoutException(String s) {
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
    public NetworkTimeoutException(String s, Throwable cause) {
        super(s, cause);
    }

    /**
     * Constructs an <code>ZorillaException</code> with the specified cause.
     * 
     * @param cause
     *            the cause
     */
    public NetworkTimeoutException(Throwable cause) {
        super(cause);
    }
  
}
