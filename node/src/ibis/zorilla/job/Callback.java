package ibis.zorilla.job;

public interface Callback {
    
    /**
     * Function called whenever a callback is needed. 
     * It is not allowed to block in this function.
     */
    public void callback();

}
