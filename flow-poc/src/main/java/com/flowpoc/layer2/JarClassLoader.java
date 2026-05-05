package com.flowpoc.layer2;

import java.io.File;
import java.net.URLClassLoader;

public class JarClassLoader {

    private final URLClassLoader classLoader;

    public JarClassLoader(File jarFile) {
        try {
            this.classLoader = new URLClassLoader(
                    new java.net.URL[]{jarFile.toURI().toURL()},
                    Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot load JAR: " + jarFile.getAbsolutePath(), e);
        }
    }

    public Class<?> load(String fullyQualifiedName) throws ClassNotFoundException {
        return classLoader.loadClass(fullyQualifiedName);
    }

    public URLClassLoader getClassLoader() {
        return classLoader;
    }
}
