package ibis.zorilla.zoni;

public interface Callback {
    
    /**
     * Function called by the callback generator whenever an update for a job
     * is available.
     */
    public void callback(JobInfo info);

}
