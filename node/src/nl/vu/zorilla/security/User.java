package nl.vu.zorilla.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.UUID;

import org.bouncycastle.openpgp.PGPPublicKey;

/**
 * Class representing a user (including signatures)
 */
public class User {

    // ** START OF CERTIFIED FIELDS **\\

    // unique id of this user
    private final UUID uuid;

    // free form string given by user (e.g. his/her name)
    private final String id;

    private final String email;

    // public key (usually rsa) of user
    private final PublicKey publicKey;

    // (open)PGP certificate of user
    private final PGPPublicKey pgpPublicKey;

    // globus certificate of user
    private final X509Certificate x509Certificate;

    private final long expirationDate;

    // ** END OF CERTIFIED FIELDS **\\

    // some signatures (self signature is manditory)

    private final byte[] selfSignature;

    private byte[] pgpSignature;

    private byte[] x509Signature;

    private MembershipCertificate[] memberships;

    // private key (if available)
    private PrivateKey privateKey;

    public User(UUID uuid, String id, String email, String pgpCertificateFile,
        String x509CertificateFile, long expirationDate) {
        this.uuid = uuid;
        this.id = id;
        this.email = email;
        this.expirationDate = expirationDate;

        KeyPair keypair = generateKeyPair();

        this.publicKey = keypair.getPublic();
        this.privateKey = keypair.getPrivate();

        pgpPublicKey = loadPGPCertificate(pgpCertificateFile);

        x509Certificate = loadX509Certificate(x509CertificateFile);

        selfSignature = selfSign();
    }
    
    private void encode(OutputStream out) throws IOException {
      
        //TODO: create
        
    }
    
    private byte[] encode() {
     //TODO: create
        
        return null;
        
        
    }

    private KeyPair generateKeyPair() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            return generator.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new Error("could not load RSA algorithmn");
        }
    }

    private PGPPublicKey loadPGPCertificate(String pgpCertificateFile) {
        // TODO Auto-generated method stub
        return null;
    }

    private X509Certificate loadX509Certificate(String certificateFile) {
        // TODO Auto-generated method stub
        return null;
    }

    private byte[] selfSign() {
        // TODO Auto-generated method stub
        return null;
    }
    
    private byte[] readFile(String fileName) throws Exception {
        File file = new File(fileName);

        int length = (int) file.length();

        if (length == 0L) {
            throw new Exception("file " + fileName
                + " does not exist or is empty");
        }

        try {
            FileInputStream in = new FileInputStream(file);

            byte[] result = new byte[length];

            int position = 0;

            while (position < length) {
                long bytesRead = in
                    .read(result, position, length - position);

                position += bytesRead;
            }
            
            return result;
        } catch (IOException e) {
            throw new Exception("exception opening or reading file", e);
        }
    }

    public synchronized void attachPgpSignature(String signatureFile) throws Exception {
        byte[] signature = readFile(signatureFile);

        if (!validPGPSignature(signature)) {
            throw new Exception("invalid pgp signature in file: " + signatureFile);
        }
        
        this.pgpSignature = signature;
    }

    public synchronized void attachX509Signature(String signatureFile) throws Exception {
        byte[] signature = readFile(signatureFile);

        if (!validX509Signature(signature)) {
            throw new Exception("invalid x509 signature in file: " + signatureFile);
        }
        
        this.x509Signature = signature;

    }
    
    private boolean validSelfSignature() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean valid() {
        if (!validSelfSignature()) {
            return false;
        }
        
        if (pgpPublicKey != null) {
            if (!validPGPSignature(pgpSignature)) {
                return false;
            }
        }
        
        if (x509Certificate != null) {
            if (!validX509Signature(x509Signature)) {
                return false;
            }
        }
        return true;
    }

    public PublicKey publicKey() {
        return publicKey;
    }

    public PrivateKey privateKey() {
        return privateKey;
    }
    
    private boolean validPGPSignature(byte[] signature) {
        // TODO Auto-generated method stub
        return false;
    }
    
    private boolean validX509Signature(byte[] signature) {
        // TODO Auto-generated method stub
        return false;
    }
}