package org.bgi.flexlab.gaea.util;

import java.util.ArrayList;
import java.util.List;

import org.bgi.flexlab.gaea.data.exception.UserException;

public class NestedObjectArray<T> {
	private int size;
	private int[] maximumArray;
	private Object[] data;
	
	public NestedObjectArray(final int... elements){
		size = elements.length;
		
		if(size == 0)
			throw new UserException("nested object array element size must more than 0.!");
		
		maximumArray = elements.clone();
		data = new Object[elements[0]];
	}
	
	public void put(T value,int... keys){
		if(keys.length != size)
			throw new UserException("element size must equal to "+size);
		int length = size - 1;
		
		Object[] myData = data;
		
		for(int i  = 0 ; i < length ; i++){
			if(keys[i] >= maximumArray[i])
				throw new RuntimeException("key must less than "+ maximumArray[i]);
			
			Object[] temp = (Object[]) myData[keys[i]];
			
			if(temp == null){
				temp = new Object[maximumArray[i+1]];
				myData[keys[i]] = temp;
			}
			
			myData = temp;
		}
		
		myData[keys[length]] = value;
	}
	
	@SuppressWarnings("unchecked")
	public T get(final int... keys) {
        final int length = size - 1;
        Object[] myData = data;

        for( int i = 0; i < length; i++ ) {
            if ( keys[i] >= maximumArray[i] )
                return null;
            myData = (Object[])myData[keys[i]];
            if ( myData == null )
                return null;
        }
        return (T)myData[keys[length]];
    }
	
	public List<T> getAllValues() {
        final List<T> result = new ArrayList<T>();
        fillAllValues(data, result);
        return result;
    }

    @SuppressWarnings("unchecked")
	private void fillAllValues(final Object[] array, final List<T> result) {
        for ( Object value : array ) {
            if ( value == null )
                continue;
            if ( value instanceof Object[] )
                fillAllValues((Object[])value, result);
            else
                result.add((T)value);
        }
    } 

    public static class Leave {
        public final int[] keys;
        public final Object value;

        public Leave(final int[] keys, final Object value) {
            this.keys = keys;
            this.value = value;
        }
    }

    public List<Leave> getAllLeaves() {
        final List<Leave> result = new ArrayList<Leave>();
        fillAllLeaves(data, new int[0], result);
        return result;
    }

    private void fillAllLeaves(final Object[] array, final int[] path, final List<Leave> result) {
        for ( int key = 0; key < array.length; key++ ) {
            final Object value = array[key];
            if ( value == null )
                continue;
            final int[] newPath = appendToPath(path, key);
            if ( value instanceof Object[] ) {
                fillAllLeaves((Object[]) value, newPath, result);
            } else {
                result.add(new Leave(newPath, value));
            }
        }
    }

    private int[] appendToPath(final int[] path, final int newKey) {
        final int[] newPath = new int[path.length + 1];
        for ( int i = 0; i < path.length; i++ )
            newPath[i] = path[i];
        newPath[path.length] = newKey;
        return newPath;
    }
}
