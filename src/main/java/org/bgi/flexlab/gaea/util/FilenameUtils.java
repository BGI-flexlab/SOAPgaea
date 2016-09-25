package org.bgi.flexlab.gaea.util;

import java.io.File;

public class FilenameUtils {

	
	
	 /**
     * The extension separator character.
     */
    private static final char EXTENSION_SEPARATOR = '.';

    /**
     * The Unix separator character.
     */
    private static final char UNIX_SEPARATOR = '/';

    /**
     * The Windows separator character.
     */
    private static final char WINDOWS_SEPARATOR = '\\';

    /**
     * The system separator character.
     */
    private static final char SYSTEM_SEPARATOR = File.separatorChar;

    /**
     * The separator character that is the opposite of the system separator.
     */
    @SuppressWarnings("unused")
	private static final char OTHER_SEPARATOR;
    static {
        if (SYSTEM_SEPARATOR == WINDOWS_SEPARATOR) {
            OTHER_SEPARATOR = UNIX_SEPARATOR;
        } else {
            OTHER_SEPARATOR = WINDOWS_SEPARATOR;
        }
    }
	
	public static String getBaseName(String filename) {
        return removeExtension(getName(filename));
    }
	
	
	public static String getName(String filename) {
        if (filename == null) {
            return null;
        }
        int index = indexOfLastSeparator(filename);
        return filename.substring(index + 1);
    }
	
	public static String removeExtension(String filename) {
        if (filename == null) {
            return null;
        }
        int index = indexOfExtension(filename);
        if (index == -1) {
            return filename;
        } else {
            return filename.substring(0, index);
        }
    }


	public static int indexOfLastSeparator(String filename) {
        if (filename == null) {
            return -1;
        }
        int lastUnixPos = filename.lastIndexOf(UNIX_SEPARATOR);
        int lastWindowsPos = filename.lastIndexOf(WINDOWS_SEPARATOR);
        return Math.max(lastUnixPos, lastWindowsPos);
    }
	
	 public static int indexOfExtension(String filename) {
	        if (filename == null) {
	            return -1;
	        }
	        int extensionPos = filename.lastIndexOf(EXTENSION_SEPARATOR);
	        int lastSeparator = indexOfLastSeparator(filename);
	        return (lastSeparator > extensionPos ? -1 : extensionPos);
	    }
	
}
