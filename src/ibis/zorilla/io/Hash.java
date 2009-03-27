package ibis.zorilla.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
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
    
    public Hash(File file)
        throws IOException {
        MessageDigest digest;
        
        FileInputStream in = new FileInputStream(file);

        try {
            digest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("could not load SHA-1 algorithmn");
        }

        byte[] buffer = new byte[1024];
        
        while (true) {
            int read = in.read(buffer);
            if (read == -1) {
                //EOF
                hash = digest.digest();
                return;
            }
            digest.update(buffer, 0, read);
        }
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
    
    public String toString() {
        return new BigInteger(hash).toString(16);
    }

}
