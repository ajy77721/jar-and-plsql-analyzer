package com.jaranalyzer.util;

public final class TypeUtils {

    private TypeUtils() {}

    public static String cleanTypeName(String typeName) {
        if (typeName == null) return "void";
        return typeName
                .replace("java.lang.", "")
                .replace("java.util.", "")
                .replace("java.io.", "")
                .replace("java.math.", "")
                .replace("java.time.", "");
    }

    public static String accessModifier(int accessFlags) {
        if ((accessFlags & 0x0001) != 0) return "public";
        if ((accessFlags & 0x0004) != 0) return "protected";
        if ((accessFlags & 0x0002) != 0) return "private";
        return "package-private";
    }

    public static boolean isStatic(int accessFlags) {
        return (accessFlags & 0x0008) != 0;
    }

    public static boolean isFinal(int accessFlags) {
        return (accessFlags & 0x0010) != 0;
    }
}
