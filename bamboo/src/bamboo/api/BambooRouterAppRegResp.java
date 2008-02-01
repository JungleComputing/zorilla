/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;
import java.math.BigInteger;
import seda.sandStorm.api.QueueElementIF;
import bamboo.util.GuidTools;

/**
 * BambooRouterAppRegResp.
 *
 * @author  Sean C. Rhea
 * @version $Id$
 */
public class BambooRouterAppRegResp implements QueueElementIF {

    public long app_id;
    public boolean success;
    public String msg;
    public BigInteger modulus;
    public int guid_digits;
    public int digit_values;
    public BigInteger node_guid;

    public BambooRouterAppRegResp (long a, boolean s, 
	    BigInteger mod, int g, int d, BigInteger ng) { 
	app_id = a;  success = s; modulus = mod;  guid_digits = g;  
	digit_values = d; node_guid = ng;
    }

    public BambooRouterAppRegResp (long a, boolean s, String m) { 
	app_id = a;  success = s;  msg = m;
    }

    public String toString () {
	return "(BambooRouterAppRegResp app_id=" + Long.toHexString (app_id) + 
	    " success=" + success + " msg=" + msg + " node_guid=" + 
	    GuidTools.guid_to_string (node_guid) + ")";
    }
}

