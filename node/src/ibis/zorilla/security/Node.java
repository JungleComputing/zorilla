package ibis.zorilla.security;

import java.security.PublicKey;
import java.util.UUID;

public class Node {
    
    //** START OF CERTIFIED FIELDS **\\
    
    UUID id;
    
    User owner;
    
    //public key of this node (rsa key)
    PublicKey publicKey;
    
    long expirationDate;
    
    //** END OF CERTIFIED FIELDS **\\
    
    byte[] selfSignature;
    
    byte[] ownerSignature;
    
}
