package org.bgi.flexlab.gaea.data.structure.filetype;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GZipType {
	private static String MAGIC = "1F8B08";
	
	private static String getFileHeader(String filePath) {
        FileInputStream is = null;
        String value = null;
        try {
            is = new FileInputStream(filePath);
            byte[] b = new byte[3];
            is.read(b, 0, b.length);
            value = bytesToHexString(b);
        } catch (Exception e) {
        } finally {
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                }
            }
        }
        return value;
    }
	
	private static String getFileHeader(Path file) {
		FileSystem fs = null;
		FSDataInputStream ins = null;
		String value = null;
		try {
			fs = file.getFileSystem(new Configuration());
			ins = fs.open(file);
			byte[] b = new byte[3];
			ins.read(b, 0, b.length);
			value = bytesToHexString(b);
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		} finally {
            if (null != ins) {
                try {
                    ins.close();
                } catch (IOException e) {
                }
            }
        }
		
		return value;
	}

    private static String bytesToHexString(byte[] src) {
        StringBuilder builder = new StringBuilder();
        if (src == null || src.length <= 0) {
            return null;
        }
        String hv;
        for (int i = 0; i < src.length; i++) {
            hv = Integer.toHexString(src[i] & 0xFF).toUpperCase();
            if (hv.length() < 2) {
                builder.append(0);
            }
            builder.append(hv);
        }
        return builder.toString();
    }
    
    public static boolean isGzip(String fileName) {
    	if (getFileHeader(fileName).equals(MAGIC))
    		return true;
    	return false;
    }
    
    public static boolean isGzip(Path file) {
    	if (getFileHeader(file).equals(MAGIC))
    		return true;
    	return false;
    }
}
