/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.layer2;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

public class JarClassLoader {
    private final URLClassLoader classLoader;

    public JarClassLoader(File jarFile) {
        try {
            this.classLoader = new URLClassLoader(new URL[]{jarFile.toURI().toURL()}, Thread.currentThread().getContextClassLoader());
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Cannot load JAR: " + jarFile.getAbsolutePath(), e);
        }
    }

    public Class<?> load(String fullyQualifiedName) throws ClassNotFoundException {
        return this.classLoader.loadClass(fullyQualifiedName);
    }

    public URLClassLoader getClassLoader() {
        return this.classLoader;
    }
}

