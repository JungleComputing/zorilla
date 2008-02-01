/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;

import ostore.util.NodeId;
import seda.sandStorm.api.QueueElementIF;

public class BambooDownNodeAdd implements QueueElementIF {
    
    public NodeId node_id;

    public BambooDownNodeAdd(NodeId node_id) {
        this.node_id = node_id;
    }

    public String toString() {
        return "(BambooDownNodeAdd node_id=" + node_id;
    }
}
