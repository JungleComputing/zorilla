package nl.vu.zorilla.bamboo;

import org.apache.log4j.Logger;

import ibis.util.ThreadPool;
import nl.vu.zorilla.JobAdvert;
import nl.vu.zorilla.Node;

/**
 * @author Niels Drost
 * 
 */
final class MessageHandler implements Runnable {

    private static final Logger logger = Logger.getLogger(MessageHandler.class);

    private final BambooMessage message;

    private final Node node;

    public MessageHandler(BambooMessage message, Node node) {
        this.message = message;
        this.node = node;

        ThreadPool.createNew(this, "message upcall thread");

    }

    public void run() {
        try {
            switch (message.getType()) {
            case JOB_ADVERT:
                JobAdvert advert = (JobAdvert) message.readObject();
                node.handleJobAdvert(advert);
                break;
            case NETWORK_KILL:
                node.handleNetworkKill();
            default:
                logger.error("message handler created for "
                        + "unsupported message type", new Exception());
            }
        } catch (Exception e) {
            logger.error("could not read advert", e);
        }

    }
}