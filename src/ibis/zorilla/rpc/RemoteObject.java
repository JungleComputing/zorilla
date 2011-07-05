package ibis.zorilla.rpc;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RemoteObject {

    private static final Logger logger = LoggerFactory
            .getLogger(RemoteObject.class);

    @SuppressWarnings("rawtypes")
    private final Class interfaceClass;

    private final Object theObject;

    private final String name;

    public String getName() {
        return name;
    }

    <InterfaceType extends Object> RemoteObject(
            Class<InterfaceType> interfaceClass, InterfaceType theObject,
            String name) throws RemoteException {
        this.interfaceClass = interfaceClass;
        this.theObject = theObject;
        this.name = name;

        // check if all methods of given interface throw a RemoteException
        for (Method method : interfaceClass.getDeclaredMethods()) {
            boolean found = false;
            for (Class<?> clazz : method.getExceptionTypes()) {
                if (clazz.equals(RemoteException.class)) {
                    found = true;
                }
            }
            if (!found) {
                throw new RemoteException("required RemoteException not thrown"
                        + " by remote method \"" + method.getName()
                        + "\" in remote object interface \""
                        + interfaceClass.getName() + "\"");
            }
        }

        logger.debug("remote object " + this + " created");
    }

    /**
     * Function called by Ibis to give us a newly arrived message. Not meant to
     * be called by users.
     */
    @SuppressWarnings("unchecked")
    public void invoke(ObjectInputStream in, ObjectOutputStream out)
            throws IOException, ClassNotFoundException {

        // read request
        String methodName = in.readUTF();
        Class<?>[] parameterTypes = (Class<?>[]) in.readObject();
        Object[] args = (Object[]) in.readObject();

        if (logger.isDebugEnabled()) {
            logger.debug("received invocation for remote object. name = "
                    + name + ", method name =  " + methodName);
        }

        boolean success;
        Object result = null;
        try {
            Method method = interfaceClass.getDeclaredMethod(methodName,
                    parameterTypes);

            result = method.invoke(theObject, args);
            success = true;
        } catch (Throwable exception) {
            // method threw an exception, return to caller
            result = exception;
            success = false;
        }

        // send reply message
        out.writeBoolean(success);
        out.writeObject(result);
    }

    public String toString() {
        return name;
    }

}
