package org.bgi.flexlab.gaea.tools.recalibrator;

import org.bgi.flexlab.gaea.data.exception.UserException;

public enum EventType {
    SNP(0, "M"),
    Insertion(1, "I"),
    Deletion(2, "D");

    public final int index;
    private final String representation;

    private EventType(int index, String representation) {
        this.index = index;
        this.representation = representation;
    }

    public static EventType eventFrom(int index) {
        switch (index) {
            case 0:
                return SNP;
            case 1:
                return Insertion;
            case 2:
                return Deletion;
            default:
                throw new UserException(String.format("Event %d does not exist.", index));
        }        
    }
    
    public static EventType eventFrom(String event) {
        for (EventType eventType : EventType.values())
            if (eventType.representation.equals(event))
                return eventType;
        throw new UserException(String.format("Event %s does not exist.", event));
    }

    @Override
    public String toString() {
        return representation;
    }
}
