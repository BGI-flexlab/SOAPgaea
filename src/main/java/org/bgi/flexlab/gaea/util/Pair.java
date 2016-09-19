package org.bgi.flexlab.gaea.util;

public class Pair<X, Y> {
	// declare public, STL-style for easier and more efficient access:
	public X first;
	public Y second;

	public Pair(X x, Y y) {
		first = x;
		second = y;
	}

	public void set(X x, Y y) {
		first = x;
		second = y;
	}

	public X getFirst() {
		return first;
	}

	public Y getSecond() {
		return second;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object object) {
		if (object == null){
			return false;
		}
		if (!(object instanceof Pair)){
			return false;
		}

		Pair other = (Pair) object;

		// Check to see whether one is null but not the other.
		if (this.first == null && other.first != null){
			return false;
		}
		if (this.second == null && other.second != null){
			return false;
		}

		// Check to see whether the values are equal.
		if (this.first != null && !this.first.equals(other.first)){
			return false;
		}
		if (this.second != null && !this.second.equals(other.second)){
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		if (second == null && first == null){
			return 0;
		}
		if (second == null){
			return first.hashCode();
		}
		if (first == null){
			return second.hashCode();
		}
		return first.hashCode() ^ second.hashCode();
	}

	public String toString() {
		return first + "," + second;
	}
}