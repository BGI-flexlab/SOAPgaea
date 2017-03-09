package org.bgi.flexlab.gaea.tools.annotator.db;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class Results extends HashMap<String, LinkedList<HashMap<String,String>>> {

	private static final long serialVersionUID = -8860279543165990227L;

	public Results() {
		super();
	}

	/**
	 * Add multiple values
	 */
	public void add(String key, Collection<HashMap<String,String>> values) {
		getOrCreate(key).addAll(values); // Add all to the list
	}

	/**
	 * Add a single value
	 */
	public void add(String key, HashMap<String,String> value) {
		getOrCreate(key).add(value); // Add to the list
	}

	/**
	 * Get a list of values (or create it if not available)
	 */
	public List<HashMap<String,String>> getOrCreate(String key) {
		// Get list
		LinkedList<HashMap<String,String>> list = get(key);
		if (list == null) { // No list? Create one
			list = new LinkedList<HashMap<String,String>>();
			put(key, list);
		}
		return list;
	}
}
