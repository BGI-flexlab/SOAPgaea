package org.bgi.flexlab.gaea.util;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class FragmentCollection<T> {
	Collection<T> singletons;
    Collection<List<T>> overlappingPairs;

    public FragmentCollection(final Collection<T> singletons, final Collection<List<T>> overlappingPairs) {
        this.singletons = singletons == null ? Collections.<T>emptyList() : singletons;
        this.overlappingPairs = overlappingPairs == null ? Collections.<List<T>>emptyList() : overlappingPairs;
    }

    /**
     * Gets the T elements not containing overlapping elements, in no particular order
     *
     * @return
     */
    public Collection<T> getSingletonReads() {
        return singletons;
    }

    /**
     * Gets the T elements containing overlapping elements, in no particular order
     *
     * @return
     */
    public Collection<List<T>> getOverlappingPairs() {
        return overlappingPairs;
    }
}
