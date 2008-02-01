/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;

import ostore.util.NodeId;
import seda.sandStorm.api.QueueElementIF;


public class BambooNeighbors implements QueueElementIF {

  public NodeId[] neighbors;
    
  public BambooNeighbors(NodeId[] neighbors) {
      this.neighbors = neighbors.clone();
  }
  
  public String toString() {
      String result = "";
      
      for (int i = 0; i < neighbors.length; i++) {
          result += neighbors[i] + ", ";
      }
      
      return result;
  }
  
}

