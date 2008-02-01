/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;


import ostore.util.QuickSerializable;
import seda.sandStorm.api.QueueElementIF;

/**
 * Initiate a routing operation to <code>dest</code>. Place the message to be
 * sent in <code>payload</code>. To receive the message, the node responsible
 * for <code>dest</code> must have registered an application (see
 * <code>BambooRouterAppRegReq</code>) with identifier <code>app_id</code>.
 * If <code>intermediate_upcall</code> is true, a
 * <code>BambooRouteUpcall</code> event will be sent to the application on
 * each intermediate node in the path. To continue the routing operation, that
 * node must send a <code>BambooRouteContinue</code> event. Finally, a
 * <code>BambooRouteDeliver</code> event will be sent to the application once
 * the message reaches the node responsible for <code>app_id</code>.
 * 
 * @author Sean C. Rhea
 * @version $Id$
 */
public class BambooFloodInit implements QueueElementIF {

    public long app_id;



    public QuickSerializable payload;
    
    public int metricValue;

    public BambooFloodInit(long app_id, QuickSerializable payload, int metricValue) {
        this.app_id = app_id;

        this.payload = payload;
        this.metricValue = metricValue;
        
     
    }

    public String toString() {
        return "(BambooFloodInit" 
                + " app_id=" + Long.toHexString(app_id)
             
                + " payload=" + payload + ")";
    }
}
