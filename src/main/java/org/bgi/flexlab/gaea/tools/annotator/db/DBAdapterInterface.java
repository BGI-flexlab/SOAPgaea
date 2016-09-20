package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.util.HashMap;

public interface DBAdapterInterface {
	
	public abstract void connection();
	
	public abstract void connection(String dbName);
	
	public abstract void disconnection() throws IOException;
	
	public abstract HashMap<String, String> getResult(String stableName, String condition, String[] tags);

	
}
