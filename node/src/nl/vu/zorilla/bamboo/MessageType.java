package nl.vu.zorilla.bamboo;

import nl.vu.zorilla.ZorillaException;

enum MessageType {
    NETWORK_KILL, JOB_ADVERT, CALL, RETURN_VALUE, EXCEPTION, CONNECTION_REQUEST, ACK, END_OF_STREAM, PING, PONG, NODE_ANNOUNCE, NODE_ANNOUNCE_REQUEST; 

    /**
     * returns type if this MessageType instance equals any of the given types
     */
    public boolean matches(MessageType[] types) {
        for (MessageType type : types) {
            if (type.equals(this)) {
                return true;
            }
        }
        return false;
    }

    public static MessageType fromOrdinal(int ordinal) throws ZorillaException {
        for (MessageType type: MessageType.class.getEnumConstants()) {
            if (type.ordinal() == ordinal) {
                return type;
            }
        }
        throw new ZorillaException("unknown message type: " + ordinal);
    }
}