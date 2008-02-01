package nl.vu.zorilla.bigNet.bamboo;


import ibis.util.ThreadPool;
import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaError;


/**
 * @author Niels Drost
 * 
 */
final class MessageHandler implements Runnable {

    private final BambooMessage message;

    private final Node node;


    public MessageHandler(BambooMessage message, Node node) {
        this.message = message;
        this.node = node;

        ThreadPool.createNew(this, "message upcall thread");

    }

    public void run() {
        switch (message.getType()) {
        case USER:
            node.receive(message);
            break;

        default:
            throw new ZorillaError("message handler created for "
                    + "unsupported message type");
        }

    }
}