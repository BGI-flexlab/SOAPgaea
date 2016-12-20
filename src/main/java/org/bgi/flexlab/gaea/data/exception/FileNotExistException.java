package org.bgi.flexlab.gaea.data.exception;

public class FileNotExistException extends UserException{

	private static final long serialVersionUID = -8379153613046946464L;

	public FileNotExistException(String path) {
		super(String.format("file %s is not exist!", path));
	}
	
	public static class MissingHeaderException extends UserException{

		/**
		 * 
		 */
		private static final long serialVersionUID = -1223885852313830694L;

		public MissingHeaderException(String path) {
			super(String.format("file %s is not exist header!", path));
		}
		
	}
}
