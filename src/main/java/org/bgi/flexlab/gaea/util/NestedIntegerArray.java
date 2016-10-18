
package org.bgi.flexlab.gaea.util;


import java.util.ArrayList;
import java.util.List;




public class NestedIntegerArray<T> {

    protected final Object[] data;

    protected final int numDimensions;
    protected final int[] dimensions;

    public NestedIntegerArray(final int... dimensions) {
        numDimensions = dimensions.length;
        if ( numDimensions == 0 )
            throw new RuntimeException("There must be at least one dimension to an NestedIntegerArray");
        this.dimensions = dimensions.clone();

        data = new Object[dimensions[0]];
    }

    @SuppressWarnings("unchecked")
	public T get(final int... keys) {
        final int numNestedDimensions = numDimensions - 1;
        Object[] myData = data;

        for( int i = 0; i < numNestedDimensions; i++ ) {
            if ( keys[i] >= dimensions[i] )
                return null;
            myData = (Object[])myData[keys[i]];
            if ( myData == null )
                return null;
        }
        return (T)myData[keys[numNestedDimensions]];
    }

    public synchronized void put(final T value, final int... keys) { // WARNING! value comes before the keys!
       //data[keys[0]][keys[1]=value;
    	if ( keys.length != numDimensions )
            throw new RuntimeException("Exactly " + numDimensions + " keys should be passed to this NestedIntegerArray but " + keys.length + " were provided");

        final int numNestedDimensions = numDimensions - 1;//1
        Object[] myData = data; // data = new Object[readgroups]
        for ( int i = 0; i < numNestedDimensions; i++ ) {
            if ( keys[i] >= dimensions[i] )
                throw new RuntimeException("Key " + keys[i] + " is too large for dimension " + i + " (max is " + (dimensions[i]-1) + ")");
            Object[] temp = (Object[])myData[keys[i]];
            if ( temp == null ) {
                temp = new Object[dimensions[i+1]];
                myData[keys[i]] = temp;
            }
            myData = temp;
        }

        myData[keys[numNestedDimensions]] = value;
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

    public static class Leaf {
        public final int[] keys;
        public final Object value;

        public Leaf(final int[] keys, final Object value) {
            this.keys = keys;
            this.value = value;
        }
    }

    public List<Leaf> getAllLeaves() {
        final List<Leaf> result = new ArrayList<Leaf>();
        fillAllLeaves(data, new int[0], result);
        return result;
    }

    private void fillAllLeaves(final Object[] array, final int[] path, final List<Leaf> result) {
        for ( int key = 0; key < array.length; key++ ) {
            final Object value = array[key];
            if ( value == null )
                continue;
            final int[] newPath = appendToPath(path, key);
            if ( value instanceof Object[] ) {
                fillAllLeaves((Object[]) value, newPath, result);
            } else {
                result.add(new Leaf(newPath, value));
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

