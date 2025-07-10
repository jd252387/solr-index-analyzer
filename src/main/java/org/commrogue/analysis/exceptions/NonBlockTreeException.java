package org.commrogue.analysis.exceptions;

public class NonBlockTreeException extends Exception {
    public NonBlockTreeException(String field, String segmentName) {
        super("Field " + field + " in segment " + segmentName
                + "does not map to an start BlockTermState, and allowNonBlockTermState is no enabled.");
    }
}
