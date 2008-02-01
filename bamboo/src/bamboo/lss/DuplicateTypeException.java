/*
 * Copyright (c) 2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;

/**
 * Thrown when a second or subsequent handler is registered for a message
 * type.
 *
 * @author Sean C. Rhea
 * @version $Id$
 */
public class DuplicateTypeException extends Exception {
    public Class type;
    public DuplicateTypeException (Class t) { type = t; }
    public String toString () { 
        return "DuplicateTypeException: " + type.getName ();
    }
}

