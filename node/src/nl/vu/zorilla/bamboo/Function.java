package nl.vu.zorilla.bamboo;

import nl.vu.zorilla.ZorillaException;


public enum Function {


    KILL_NETWORK, 

  
    JOB_ADVERT,
    
    NODE_DISCOVERY,
   
    
    ;

    //    /**
    //     * returns type if this MessageFunction instance equals any of the given types
    //     */
    //    public boolean matches(Function[] functions) {
    //        for (Function function : functions) {
    //            if (function.equals(this)) {
    //                return true;
    //            }
    //        }
    //        return false;
    //    }
    //    public boolean isFileMessage() {
    //        Function[] functions = { FILE_REQUEST, FILE_PUSH_REQUEST, FILE_CONNECTION, FILE_BLOCK_REQUEST, FILE_BLOCK_NOT_FOUND };
    //        return matches(functions);
    //    }

    public static Function fromOrdinal(int ordinal) throws ZorillaException {
        for (Function function : Function.class.getEnumConstants()) {
            if (function.ordinal() == ordinal) {
                return function;
            }
        }
        throw new ZorillaException("unknown message function: " + ordinal);
    }
}
