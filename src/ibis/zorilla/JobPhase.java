package ibis.zorilla;

public enum JobPhase {

    UNKNOWN, INITIAL, PRE_STAGE, SCHEDULING, RUNNING, CLOSED, POST_STAGING, COMPLETED, CANCELLED, USER_ERROR, ERROR;

    public boolean isAfter(JobPhase other) {
        return this.ordinal() > other.ordinal();
    }

    public boolean atLeast(JobPhase other) {
        return this.ordinal() >= other.ordinal();
    }

    public boolean isBefore(JobPhase other) {
        return this.ordinal() < other.ordinal();
    }

    public boolean atMost(JobPhase other) {
        return this.ordinal() <= other.ordinal();
    }

}
