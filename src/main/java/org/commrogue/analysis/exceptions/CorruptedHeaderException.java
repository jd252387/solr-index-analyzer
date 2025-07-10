package org.commrogue.analysis.exceptions;

import java.io.IOException;

public class CorruptedHeaderException extends Exception {
    public CorruptedHeaderException(String fileName, IOException cause) {
        super("Encountered corrupted header for file " + fileName, cause);
    }
}
