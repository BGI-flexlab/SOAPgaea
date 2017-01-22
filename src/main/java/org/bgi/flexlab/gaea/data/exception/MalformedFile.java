package org.bgi.flexlab.gaea.data.exception;

import java.io.File;

public class MalformedFile extends UserException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3457749925016903826L;

	public MalformedFile(String message) {
        super(String.format("Unknown file is malformed: %s", message));
    }

    public MalformedFile(String message, Exception e) {
        super(String.format("Unknown file is malformed: %s caused by %s", message, getMessage(e)));
    }

    public MalformedFile(File f, String message) {
        super(String.format("File %s is malformed: %s", f.getAbsolutePath(), message));
    }

    public MalformedFile(File f, String message, Exception e) {
        super(String.format("File %s is malformed: %s caused by %s", f.getAbsolutePath(), message, getMessage(e)));
    }

    public MalformedFile(String name, String message) {
        super(String.format("File associated with name %s is malformed: %s", name, message));
    }

    public MalformedFile(String name, String message, Exception e) {
        super(String.format("File associated with name %s is malformed: %s caused by %s", name, message, getMessage(e)));
    }
}
