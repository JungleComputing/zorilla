/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;
import seda.sandStorm.api.QueueElementIF;

/**
 * BambooRoutingTableChanged.
 *
 * @author  Sean C. Rhea
 * @version $Id$
 */
public class BambooRoutingTableChanged implements QueueElementIF {

    public BambooNeighborInfo [] added;
    public BambooNeighborInfo [] removed;

    public BambooRoutingTableChanged (
	    BambooNeighborInfo [] a, BambooNeighborInfo [] r) { 
	added = a;
	removed = r;
    }

    public String toString () {
	String result = "(BambooRoutingTableChanged added=(";
	if (added != null) {
	    for (int i = added.length - 1; i >= 0; --i) 
		result += added [i] + ((i == 0) ? "" : " ");
	}
	result += ") removed=(";
	if (removed != null) {
	    for (int i = removed.length - 1; i >= 0; --i) 
		result += removed [i] + ((i == 0) ? "" : " ");
	}
	return result + "))";
    }
}

