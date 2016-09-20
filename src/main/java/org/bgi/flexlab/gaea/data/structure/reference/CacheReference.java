package org.bgi.flexlab.gaea.data.structure.reference;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class CacheReference {
	
	public static boolean disCacheRef(String chrList, Configuration conf) throws IOException, URISyntaxException {
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI(chrList+ "#" + "refList"), conf);
		
		Path refPath = new Path(chrList);
		FileSystem fs = refPath.getFileSystem(conf);
		FSDataInputStream refin = fs.open(refPath);
		LineReader in = new LineReader(refin);
		Text line = new Text();
		
		String chrFile = "";
		String[] chrs = new String[3];
		while((in.readLine(line)) != 0){
			chrFile = line.toString();
			chrs = chrFile.split("\t");
			File fileTest = new File(chrs[1]);
			if(fileTest.isFile()) {
				chrs[1] = "file://" + chrs[1];
			}
			DistributedCache.addCacheFile(new URI(chrs[1] + "#" + chrs[0] + "ref"), conf);
		}
		in.close();
		refin.close();
		System.out.println("> Distributed cached reference done.");
		conf.setBoolean("cacheref", true);
		return true;
	}
	
}
