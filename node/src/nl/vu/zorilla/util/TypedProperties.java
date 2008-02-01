/* $Id$ */

package nl.vu.zorilla.util;

import java.io.PrintStream;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Utility to extract and check typed properties
 */
public class TypedProperties extends Properties {

    private static final long serialVersionUID = 1L;

    // 1.4 code
    @SuppressWarnings("unchecked")
    public TypedProperties(Map initialValues) {
        putAll(initialValues);
    }

    public TypedProperties() {
        super();
    }

    public TypedProperties(Properties defaults) {
        super(defaults);
    }

    /**
     * Returns true if property <code>name</code> is defined and has a value
     * that is conventionally associated with 'true' (as in Ant): any of 1, on,
     * true, yes.
     * 
     * @return true if property is defined and set
     * @param name
     *            property name
     */
    public boolean booleanProperty(String name) {
        return booleanProperty(name, false);
    }

    /**
     * Returns true if property <code>name</code> has a value that is
     * conventionally associated with 'true' (as in Ant): any of 1, on, true,
     * yes. If the property is not defined, return the specified default value.
     * 
     * @return true if property is defined and set
     * @param key
     *            property name
     * @param defaultValue
     *            the value that is returned if the property is absent
     */
    public boolean booleanProperty(String key, boolean defaultValue) {
        String value = getProperty(key);

        if (value != null) {
            return value.equals("1") || value.equals("on") || value.equals("")
                || value.equals("true") || value.equals("yes");
        }

        return defaultValue;
    }

    /**
     * Returns the integer value of property
     * 
     * @return the integer value of property
     * @param key
     *            property name
     * @throws NumberFormatException
     *             if the property is undefined or not an integer
     */
    public int getIntProperty(String key) {
        String value = getProperty(key);

        if (value == null) {
            throw new NumberFormatException("property undefined: " + key);
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Integer expected for property "
                + key + ", not \"" + value + "\"");
        }
    }

    /**
     * Returns the integer value of property
     * 
     * @return the integer value of property
     * @param key
     *            property name
     * @param defaultValue
     *            default value if the property is undefined
     * @throws NumberFormatException
     *             if the property defined and not an integer
     */
    public int getIntProperty(String key, int defaultValue) {
        String value = getProperty(key);

        if (value == null) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Integer expected for property "
                + key + ", not \"" + value + "\"");
        }
    }

    /**
     * Returns the long value of property
     * 
     * @return the long value of property
     * @param key
     *            property name
     * @throws NumberFormatException
     *             if the property is undefined or not an long
     */
    public long getLongProperty(String key) {
        String value = getProperty(key);

        if (value == null) {
            throw new NumberFormatException("property undefined: " + key);
        }

        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Long expected for property " + key
                + ", not \"" + value + "\"");
        }
    }

    /**
     * Returns the long value of property
     * 
     * @return the long value of property
     * @param key
     *            property name
     * @param defaultValue
     *            default value if the property is undefined
     * @throws NumberFormatException
     *             if the property defined and not an Long
     */
    public long getLongProperty(String key, long defaultValue) {
        String value = getProperty(key);

        if (value == null) {
            return defaultValue;
        }

        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Long expected for property " + key
                + ", not \"" + value + "\"");
        }
    }

    /**
     * Returns the short value of property
     * 
     * @return the short value of property
     * @param key
     *            property name
     * @throws NumberFormatException
     *             if the property is undefined or not an short
     */
    public short getShortProperty(String key) {
        String value = getProperty(key);

        if (value == null) {
            throw new NumberFormatException("property undefined: " + key);
        }

        try {
            return Short.parseShort(value);
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Short expected for property "
                + key + ", not \"" + value + "\"");
        }
    }

    /**
     * Returns the short value of property
     * 
     * @return the short value of property
     * @param key
     *            property name
     * @param defaultValue
     *            default value if the property is undefined
     * @throws NumberFormatException
     *             if the property defined and not an Short
     */
    public short getShortProperty(String key, short defaultValue) {
        String value = getProperty(key);

        if (value == null) {
            return defaultValue;
        }

        try {
            return Short.parseShort(value);
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Short expected for property "
                + key + ", not \"" + value + "\"");
        }
    }

    /**
     * Returns the long value of a size property. Valid values for the property
     * are a long, a long followed by K, a long followed by M or a long followed
     * by G. Size modifiers multiply the value by 1024, 1024^2 and 1024^3
     * respectively.
     * 
     * @return the size value of property
     * @param key
     *            property name
     * @throws NumberFormatException
     *             if the property is undefined or not a valid size
     */
    public long getSizeProperty(String key) {
        String value = getProperty(key);

        if (value == null) {
            throw new NumberFormatException("property undefined: " + key);
        }

        return getSizeProperty(key, 0);
    }

    /**
     * Returns the long value of a size property. Valid values for the property
     * are a long, a long followed by K, a long followed by M or a long followed
     * by G. Size modifiers multiply the value by 1024, 1024^2 and 1024^3
     * respectively. returns the default value if the property is undefined
     * 
     * @return the size value of property
     * @param key
     *            property name
     * @throws NumberFormatException
     *             if the property is not a valid size
     */
    public long getSizeProperty(String key, long defaultValue) {
        String value = getProperty(key);

        if (value == null) {
            return defaultValue;
        }

        try {

            if (value.endsWith("G") || value.endsWith("g")) {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 1024 * 1024 * 1024;
            }

            if (value.endsWith("M") || value.endsWith("m")) {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 1024 * 1024;
            }

            if (value.endsWith("K") || value.endsWith("k")) {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 1024;
            }

            return Long.parseLong(value);

        } catch (NumberFormatException e) {
            throw new NumberFormatException(
                "Long[G|g|M|m|K|k] expected for property " + key + ", not \""
                    + value + "\"");
        }
    }

    /**
     * Returns true if property name is defined and has a string value that
     * equals match.
     * 
     * @return true if property is defined and equals match
     * @param key
     *            property name
     * @param match
     *            value to be matched
     */
    public boolean stringPropertyMatch(String key, String match) {
        String value = getProperty(key);
        return value != null && value.equals(match);
    }

    private static boolean contains(String[] list, String element) {
        if (list == null) {
            return false;
        }
        for (int i = 0; i < list.length; i++) {
            if (element.equalsIgnoreCase(list[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if the given string starts with one of the given prefixes
     */
    private static boolean startsWith(String string, String[] prefixes) {
        if (prefixes == null) {
            return false;
        }
        for (int i = 0; i < prefixes.length; i++) {
            if (string.startsWith(prefixes[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks all properties with the given prefix for validity.
     * 
     * @return a Property object containing all unrecognized properties.
     * @param prefix
     *            the prefix that should be checked
     * @param validKeys
     *            the set of valid keys within the given prefix
     * @param validPrefixes
     *            if a propery starts with one of these prefixes, it is declared
     *            valid
     * @param printWarning
     *            if true, a warning is printed to standard error for each
     *            unknown property
     */
    public TypedProperties checkProperties(String prefix, String[] validKeys,
        String[] validPrefixes, boolean printWarning) {
        TypedProperties result = new TypedProperties();

        if (prefix == null) {
            prefix = "";
        }

        Iterator iterator = entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();

            if (key.startsWith(prefix)) {
                String suffix = key.substring(prefix.length());

                if (!startsWith(suffix, validPrefixes)
                    && !contains(validKeys, suffix)) {
                    if (printWarning) {
                        System.err.println("Warning, unknown property: " + key
                            + " with value: " + value);
                    }
                    result.put(key, value);
                }
            }
        }
        return result;
    }

    /**
     * Prints properties (including default properties) to a stream.
     * 
     * @param out
     *            The stream to write output to.
     * @param prefix
     *            Only print properties which start with the given prefix. If
     *            null, will print all properties
     */
    public void printProperties(PrintStream out, String prefix) {
        if (prefix == null) {
            prefix = "";
        }

        for (Enumeration e = propertyNames(); e.hasMoreElements();) {
            String key = (String) e.nextElement();
            String value = getProperty(key);

            if (key.toLowerCase().startsWith(prefix.toLowerCase())) {
                out.println(key + " = " + value);
            }
        }
    }

    public String toVerboseString() {
        String result = "";

        for (Enumeration e = propertyNames(); e.hasMoreElements();) {
            String key = (String) e.nextElement();
            String value = getProperty(key);

            result += key + " = " + value + " ";
        }

        return result;
    }

}
