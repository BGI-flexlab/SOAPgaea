package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

public interface DBAdapterInterface {
	
	public abstract void connection(String dbName) throws IOException;
	
	public abstract void disconnection() throws IOException;
	
	public abstract HashMap<String, String> getResult(String stableName, String condition, Set<String> tags);

}
