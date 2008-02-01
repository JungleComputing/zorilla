package nl.vu.zorilla.security;

import java.security.PublicKey;
import java.util.UUID;

public class Group {
    
    //** START OF CERTIFIED FIELDS **\\
    
    private UUID uuid;
    
    private String id;

    //private PublicKey publicKey;
    
    private User[] owners;

    //** START OF CERTIFIED FIELDS **\\
    
    byte[] selfSignature;
    
    byte[][] ownerSignature;

}
