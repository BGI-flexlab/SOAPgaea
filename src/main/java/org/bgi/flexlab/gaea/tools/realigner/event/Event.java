package org.bgi.flexlab.gaea.tools.realigner.event;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;

public class Event {
	public enum EVENT_TYPE {
		POINT_EVENT, INDEL_EVENT, BOTH
	}

	private int eventStartPosition = -1;
	private int eventStopPosition = -1;
	private int furthestStopPosition;

	private GenomeLocation location;
	private ArrayList<Integer> pointEvents = null;

	private EVENT_TYPE type;

	public Event(GenomeLocation location, int furthest, EVENT_TYPE type) {
		this.type = type;
		this.furthestStopPosition = furthest;
		this.location = location;

		if (type != EVENT_TYPE.POINT_EVENT) {
			this.eventStartPosition = location.getStart();
			this.eventStopPosition = location.getStop();
		}

		pointEvents = new ArrayList<Integer>();
		if (type != EVENT_TYPE.INDEL_EVENT) {
			pointEvents.add(location.getStart());
		}
	}
	
	public String toString(){
    	return eventStartPosition+"-"+eventStopPosition+"->"+furthestStopPosition+":"+type.toString();
    }

	public GenomeLocation createLocation(GenomeLocationParser parser) {
		return parser.createGenomeLocation(location.getContig(),
				eventStartPosition, eventStopPosition);
	}

	public boolean isValidEvent(GenomeLocationParser parser, int maxIntervalSize) {
		return parser.isValidGenomeLocation(location.getContig(),
				eventStartPosition, eventStopPosition, true)
				&& (eventStopPosition - eventStartPosition < maxIntervalSize);
	}

	public GenomeLocation getLocation() {
		return this.location;
	}
	
	public GenomeLocation getLocation(GenomeLocationParser parser) {
		return parser.createGenomeLocation(location.getContig(), eventStartPosition, eventStopPosition);
	}

	public EVENT_TYPE getEventType() {
		return type;
	}

	public ArrayList<Integer> getPointEvents() {
		return this.pointEvents;
	}

	public int peek() {
		if (this.pointEvents.size() == 0)
			return -1;
		return this.pointEvents.get(0);
	}

	public int last() {
		if (this.pointEvents.size() == 0)
			return -1;
		return this.pointEvents.get(pointEvents.size() - 1);
	}

	public int getFurthestStop() {
		return this.furthestStopPosition;
	}

	public int getStop() {
		return this.eventStopPosition;
	}

	public int getStart() {
		return this.eventStartPosition;
	}

	public boolean canBeMerge(Event beMerge) {
		return getLocation().getContigIndex() == beMerge.getLocation()
				.getContigIndex()
				&& this.furthestStopPosition >= beMerge.getLocation()
						.getStart();
	}

	public void set(int start, int stop, int furthStop) {
		this.eventStartPosition = start;
		this.eventStopPosition = stop;
		this.furthestStopPosition = furthStop;
	}

	public void merge(Event event, int snpWin) {
		if (event.getEventType() != EVENT_TYPE.POINT_EVENT) {
			this.furthestStopPosition = event.getFurthestStop();
			this.eventStopPosition = event.getStop();
			if (this.eventStartPosition == -1)
				this.eventStartPosition = event.getStart();
		}
		if (event.getEventType() != EVENT_TYPE.INDEL_EVENT) {
			int newPosition = event.peek();
			
			if (pointEvents.size() > 0) {
				int lastPosition = last();
				if (newPosition - lastPosition < snpWin) {
					int minStart = eventStartPosition == -1 ? lastPosition
							: Math.min(eventStartPosition, lastPosition);
					set(minStart, Math.max(eventStopPosition, newPosition),
							event.getFurthestStop());
				} else if (eventStartPosition == -1
						&& event.getStart() != eventStartPosition) {
					set(event.getStart(), event.getStop(),
							event.getFurthestStop());
				}
			}
			pointEvents.add(newPosition);
		}
	}
}
