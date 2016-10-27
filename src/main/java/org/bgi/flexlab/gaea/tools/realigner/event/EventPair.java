package org.bgi.flexlab.gaea.tools.realigner.event;

import java.util.TreeSet;

import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;

public class EventPair {
	private Event left,right;
	private TreeSet<GenomeLocation> intervals = new TreeSet<GenomeLocation>();
	
	public EventPair(Event l,Event r){
		this.left = l;
		this.right = r;
	}
	
	public EventPair(Event l,Event r,TreeSet<GenomeLocation> set1,TreeSet<GenomeLocation> set2){
		this.left = l;
		this.right = r;
		intervals.addAll(set1);
		intervals.addAll(set2);
	}
	
	public TreeSet<GenomeLocation> getIntervals(){
		return intervals;
	}
	
	public Event getLeft(){
		return left;
	}
	
	public Event getRight(){
		return right;
	}
	
	public void setLeft(Event l){
		left = l;
	}
	
	public void setRight(Event r){
		right = r;
	}
}
