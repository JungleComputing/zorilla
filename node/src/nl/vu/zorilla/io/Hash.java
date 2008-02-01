package nl.vu.zorilla.io;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

/**
 * represents a hash value for (piece of) a file
 */
public final class Hash implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger.getLogger(Hash.class);

    private final byte[] hash;

    public Hash(FileChannel channel, long position, long limit)
        throws IOException {
        MessageDigest digest;

        try {
            digest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("could not load SHA-1 algorithmn");
        }

        byte[] buffer = new byte[(int) (limit - position)];

        ByteBuffer nioBuffer = ByteBuffer.wrap(buffer);

        while (nioBuffer.hasRemaining()) {
            int read = channel.read(nioBuffer, position);
            if (read == -1) {
                throw new IOException("could not read file");
            }
            position += read;
        }
        hash = digest.digest(buffer);
    }

    public boolean equals(Object object) {
        if (object instanceof Hash) {
            return equals((Hash) object);
        }
        return false;
    }

    public boolean equals(Hash other) {
        if (other.hash.length != hash.length) {
            logger.warn("compares two different size hashes");
            return false;
        }
        for (int i = 0; i < hash.length; i++) {
            if (hash[i] != other.hash[i]) {
                return false;
            }
        }
        return true;
    }

}
