package nl.vu.zorilla.bamboo;

import nl.vu.zorilla.ZorillaException;

class NetworkException extends ZorillaException {

    private static final long serialVersionUID = 1L;

    public NetworkException() {
        super();
    }

    /**
     * Constructs an <code>ZorillaException</code> with the specified detail
     * message.
     * 
     * @param s
     *            the detail message
     */
    public NetworkException(String s) {
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
    public NetworkException(String s, Throwable cause) {
        super(s, cause);
    }

    /**
     * Constructs an <code>ZorillaException</code> with the specified cause.
     * 
     * @param cause
     *            the cause
     */
    public NetworkException(Throwable cause) {
        super(cause);
    }

}
