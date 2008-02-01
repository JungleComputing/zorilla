package nl.vu.zorilla.security;

/**
 * Class representing a "proof" of group membership of a user
 */
public class MembershipCertificate {

    //** START OF CERTIFIED FIELDS **\\

    private User user;
    
    private Group group;
    
    private long expirationDate;

    //** END OF CERTIFIED FIELDS **\\
    
    private byte[] userSignature;
    
    private byte[] groupSignature;
    
    public boolean valid() {
        //FIXME: implement
        return false;
    }
    
}
