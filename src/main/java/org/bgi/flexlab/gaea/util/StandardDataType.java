package org.bgi.flexlab.gaea.util;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum StandardDataType {

	/**
	 * The default value when a format string is not present
	 */
	Unknown("Unknown"),

	/**
	 * Used for boolean values.
	 */
	Boolean("%[Bb]"),

	/**
	 * Used for char values.
	 */
	Character("%[Cc]"),

	/**
	 * Used for float and double values.
	 */
	Decimal("%.*[EeFf]"),

	/**
	 * Used for int, byte, short, and long values.
	 */
	Integer("%[Dd]"),

	/**
	 * Used for string values.
	 */
	String("%[Ss]");

	private final String typeDescription;

	private StandardDataType(String dataTypeString) {
		this.typeDescription = dataTypeString;
	}

	private static final Map<String, StandardDataType> descriptionToType = new HashMap<String, StandardDataType>();

	static {
		for (StandardDataType s : EnumSet.allOf(StandardDataType.class))
			descriptionToType.put(s.typeDescription, s);
	}

	@Override
	public String toString() {
		return this.typeDescription;
	}

	public static StandardDataType fromObject(Object object) {
		StandardDataType value;
		if (object instanceof Boolean) {
			value = Boolean;
		} else if (object instanceof Character) {
			value = Character;
		} else if (object instanceof Float || object instanceof Double) {
			value = Decimal;
		} else if (object instanceof Integer || object instanceof Long || object instanceof Short
				|| object instanceof Byte) {
			value = Integer;
		} else if (object instanceof String) {
			value = String;
		} else {
			value = Unknown;
		}
		return value;
	}

	public static StandardDataType fromFormatString(String format) {
		if (format == null || format.equals(""))
			return Unknown;
		for (StandardDataType type : descriptionToType.values()) {
			if (format.matches(type.toString()))
				return type;
		}
		return Unknown;
	}

	public Object getDefaultValue() {
		switch (this) {
		case Decimal:
			return 0.0D;
		case Boolean:
			return false;
		case Character:
			return '0';
		case Integer:
			return 0L;
		case String:
			return "";
		default:
			return null;
		}
	}

	public boolean isEqual(Object a, Object b) {
		switch (this) {
		case Decimal:
		case Boolean:
		case Integer:
			return a.toString().equals(b.toString());
		case Character:
		case String:
		default:
			return a.equals(b);
		}
	}

	public Object Parse(Object obj) {
		if (obj instanceof String) {
			String str = obj.toString();
			switch (this) {
			case Decimal:
				return Double.parseDouble(str);
			case Boolean:
				return java.lang.Boolean.parseBoolean(str);
			case Integer:
				return Long.parseLong(str);
			case String:
				return str;
			case Character:
				return str.toCharArray()[0];
			default:
				return str;
			}
		} else
			return null;
	}
	
	public Object getNonStringValue(Object value){
		Object newValue = null;
		if (this.equals(Integer)) {
			try {
				newValue = Long.parseLong((String) value);
			} catch (Exception e) {
				throw new RuntimeException(e.toString());
			}
		}
		if (this.equals(Decimal)) {
			try {
				newValue = Double.parseDouble((String) value);
			} catch (Exception e) {
				throw new RuntimeException(e.toString());
			}
		}
		if (this.equals(Character) && ((String) value).length() == 1) {
			newValue = ((String) value).charAt(0);
		}
		
		return newValue;
	}

	public String getDefaultFormatString() {
		switch (this) {
		case Decimal:
			return "%.8f";
		case Boolean:
			return "%b";
		case Integer:
			return "%d";
		case String:
			return "%s";
		case Character:
			return "%c";
		default:
			return "%s";
		}
	}
}
