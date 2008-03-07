package ibis.zorilla.zoni;

public final class ZoniProtocol {

    /**
     * client protocol version
     */
    public static final int VERSION = 4;

    // only authentication so far...
    public static final int AUTHENTICATION_NONE = 0;

    public static final int DEFAULT_PORT = 5445;

    // opcodes for client-to-node commands

    public static final int OPCODE_CLOSE_CONNECTION = 0;

    public static final int OPCODE_SUBMIT_JOB = 1;

    public static final int OPCODE_GET_JOB_INFO = 2;

    public static final int OPCODE_SET_JOB_ATTRIBUTES = 3;

    public static final int OPCODE_CANCEL_JOB = 4;

    public static final int OPCODE_GET_JOB_LIST = 6;

    public static final int OPCODE_GET_NODE_INFO = 7;

    public static final int OPCODE_SET_NODE_ATTRIBUTES = 8;

    public static final int OPCODE_KILL_NODE = 9;

    public static final int OPCODE_GET_FILE_INFO = 10;

    public static final int OPCODE_GET_FILE = 11;
    
    // callbacks

    public static final int CALLBACK_JOBINFO = 0;

    // status codes

    public static final int STATUS_OK = 1;

    public static final int STATUS_DENIED = 2;

    public static final int STATUS_ERROR = 3;

    // phases of a job

    public static final int PHASE_UNKNOWN = 0;

    public static final int PHASE_INITIAL = 1;

    public static final int PHASE_PRE_STAGE = 2;

    public static final int PHASE_SCHEDULING = 3;

    public static final int PHASE_RUNNING = 4;

    public static final int PHASE_CLOSED = 5;

    public static final int PHASE_POST_STAGING = 6;

    public static final int PHASE_COMPLETED = 7;

    public static final int PHASE_CANCELLED = 8;

    public static final int PHASE_ERROR = 9;

    public static final String[] PHASES =
        { "UNKNOWN", "INITIAL", "PRE_STAGE", "SCHEDULING", "RUNNING", "CLOSED",
                "POST_STAGING", "COMPLETED", "CANCELLED", "ERROR" };

    // types of peers

    public static final int TYPE_CLIENT = 1;

    public static final int TYPE_WORKER = 2;

    public static final int MAX_BLOCK_SIZE = 32 * 1024; // bytes

}
