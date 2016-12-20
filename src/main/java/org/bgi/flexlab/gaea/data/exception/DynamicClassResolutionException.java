
package org.bgi.flexlab.gaea.data.exception;

import java.lang.reflect.InvocationTargetException;

public class DynamicClassResolutionException extends UserException {
    /**
	 * 
	 */
	private static final long serialVersionUID = -3475407871237611827L;

	@SuppressWarnings("rawtypes")
	public DynamicClassResolutionException(Class c, Exception ex) {
        super(String.format("Could not create module %s because %s caused by exception %s",
                c.getSimpleName(), moreInfo(ex), ex.getMessage()));
    }

    private static String moreInfo(Exception ex) {
        try {
            throw ex;
        } catch (InstantiationException e) {
            return "BUG: cannot instantiate class: must be concrete class";
        } catch (NoSuchMethodException e) {
            return "BUG: Cannot find expected constructor for class";
        } catch (IllegalAccessException e) {
            return "Cannot instantiate class (Illegal Access)";
        } catch (InvocationTargetException e) {
            return "Cannot instantiate class (Invocation failure)";
        } catch ( Exception e ) {
            return String.format("an exception of type %s occurred",e.getClass().getSimpleName());
        }
    }
}
