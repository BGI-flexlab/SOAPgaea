
package org.bgi.flexlab.gaea.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class NestedHashMap {

	@SuppressWarnings("rawtypes")
	public final Map data = new HashMap<Object, Object>();

    @SuppressWarnings("rawtypes")
	public Object get( final Object... keys ) {
        Map map = this.data;
        final int nestedMaps = keys.length - 1;
        for( int iii = 0; iii < nestedMaps; iii++ ) {
            map = (Map) map.get(keys[iii]);
            if( map == null ) { return null; }
        }
        return map.get(keys[nestedMaps]);
    }

    public synchronized void put( final Object value, final Object... keys ) { // WARNING! value comes before the keys!
        this.put(value, false, keys );
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	public synchronized Object put( final Object value, boolean keepOldBindingIfPresent, final Object... keys ) {
        Map map = this.data;
        final int keysLength = keys.length;
        for( int iii = 0; iii < keysLength; iii++ ) {
            if( iii == keysLength - 1 ) {
                if ( keepOldBindingIfPresent && map.containsKey(keys[iii]) ) {
                    // this code test is for parallel protection when you call put() multiple times in different threads
                    // to initialize the map.  It returns the already bound key[iii] -> value
                    return map.get(keys[iii]);
                } else {
                    // we are a new binding, put it in the map
                    map.put(keys[iii], value);
                    return value;
                }
            } else {
                Map tmp = (Map) map.get(keys[iii]);
                if( tmp == null ) {
                    tmp = new HashMap();
                    map.put(keys[iii], tmp);
                }
                map = tmp;
            }
        }

        return value; // todo -- should never reach this point
    }

    public List<Object> getAllValues() {
        final List<Object> result = new ArrayList<Object>();
        fillAllValues(data, result);
        return result;
    }

    @SuppressWarnings("rawtypes")
	private void fillAllValues(final Map map, final List<Object> result) {
        for ( Object value : map.values() ) {
            if ( value == null )
                continue;
            if ( value instanceof Map )
                fillAllValues((Map)value, result);
            else
                result.add(value);
        }
    }

    public static class Leaf {
        public final List<Object> keys;
        public final Object value;

        public Leaf(final List<Object> keys, final Object value) {
            this.keys = keys;
            this.value = value;
        }
    }

    public List<Leaf> getAllLeaves() {
        final List<Leaf> result = new ArrayList<Leaf>();
        final List<Object> path = new ArrayList<Object>();
        fillAllLeaves(data, path, result);
        return result;
    }

    @SuppressWarnings("rawtypes")
	private void fillAllLeaves(final Map map, final List<Object> path, final List<Leaf> result) {
        for ( final Object key : map.keySet() ) {
            final Object value = map.get(key);
            if ( value == null )
                continue;
            final List<Object> newPath = new ArrayList<Object>(path);
            newPath.add(key);
            if ( value instanceof Map ) {
                fillAllLeaves((Map) value, newPath, result);
            } else {
                result.add(new Leaf(newPath, value));
            }
        }
    }
}
