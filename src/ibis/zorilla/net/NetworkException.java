package ibis.zorilla.net;

public class NetworkException extends Exception {

    private static final long serialVersionUID = 1L;

    public NetworkException() {
        super();
    }

    public NetworkException(String message, Throwable cause) {
        super(message, cause);
    }

    public NetworkException(String message) {
        super(message);
    }

    public NetworkException(Throwable message) {
        super(message);
    }
    
    

}
