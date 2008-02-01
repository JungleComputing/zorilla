/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;
import seda.sandStorm.api.QueueElementIF;

/**
 * BambooLeafSetChanged.
 *
 * @author  Sean C. Rhea
 * @version $Id$
 */
public class BambooLeafSetChanged implements QueueElementIF {

    public BambooNeighborInfo [] preds;
    public BambooNeighborInfo [] succs;

    public BambooLeafSetChanged (
	    BambooNeighborInfo [] p, BambooNeighborInfo [] s) { 
	preds = p;  
	succs = s;
    }

    public String toString () {
	String result = "(BambooLeafSetChanged preds=(";
	for (int i = preds.length - 1; i >= 0; --i) 
	    result += preds [i] + ((i == 0) ? "" : " ");
	result += ") succs=";
	for (int i = 0; i < succs.length; ++i) 
	    result += succs [i] + ((i == succs.length - 1) ? "" : " ");
	return result + "))";
    }
}

