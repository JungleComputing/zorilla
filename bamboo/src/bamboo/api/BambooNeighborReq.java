/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;

import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkIF;

public class BambooNeighborReq implements QueueElementIF {

  public SinkIF comp_q;

  public BambooNeighborReq(SinkIF completion_queue) {
    comp_q = completion_queue;
  }
  
}

