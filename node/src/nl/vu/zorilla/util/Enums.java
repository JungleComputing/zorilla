package nl.vu.zorilla.util;

import nl.vu.zorilla.ZorillaException;

public final class Enums {

    public static <T extends Enum<T>> T getEnumConstant(Class<T> enumType,
            int ordinal) throws ZorillaException {
        T[] enumConstants = enumType.getEnumConstants();

        for (T enumConstant : enumConstants) {
            if (enumConstant.ordinal() == ordinal) {
                return enumConstant;
            }
        }
        throw new ZorillaException("invalid ordinal " + ordinal
                + " for enum type " + enumType.getCanonicalName());
    }
}
