/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.jaranalyzer.model.ClassInfo
 *  com.mongodb.client.MongoClient
 *  org.springframework.beans.factory.config.BeanDefinition
 *  org.springframework.beans.factory.support.BeanDefinitionBuilder
 *  org.springframework.context.support.GenericApplicationContext
 */
package com.flowpoc.layer2;

import com.flowpoc.layer2.CollectingQueryInterceptor;
import com.flowpoc.layer2.JarClassLoader;
import com.flowpoc.layer2.UniversalMongoInterceptor;
import com.jaranalyzer.model.ClassInfo;
import com.mongodb.client.MongoClient;
import java.util.List;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.context.support.GenericApplicationContext;

public class BeanFactoryLoader {
    private final JarClassLoader jarClassLoader;
    private final List<ClassInfo> classInfos;
    private final CollectingQueryInterceptor queryInterceptor;
    private final MongoClient realClient;
    private final String realDbName;

    public BeanFactoryLoader(JarClassLoader jarClassLoader, List<ClassInfo> classInfos, CollectingQueryInterceptor queryInterceptor) {
        this(jarClassLoader, classInfos, queryInterceptor, null, null);
    }

    public BeanFactoryLoader(JarClassLoader jarClassLoader, List<ClassInfo> classInfos, CollectingQueryInterceptor queryInterceptor, MongoClient realClient, String realDbName) {
        this.jarClassLoader = jarClassLoader;
        this.classInfos = classInfos;
        this.queryInterceptor = queryInterceptor;
        this.realClient = realClient;
        this.realDbName = realDbName;
    }

    public GenericApplicationContext buildContext() {
        GenericApplicationContext context = new GenericApplicationContext();
        for (ClassInfo ci : this.classInfos) {
            String stereotype = ci.getStereotype();
            if (!this.isServiceOrRepository(stereotype)) continue;
            try {
                Class<?> clazz = this.jarClassLoader.load(ci.getFullyQualifiedName());
                String beanName = ci.getSimpleName() != null ? ci.getSimpleName() : this.simpleName(ci.getFullyQualifiedName());
                context.registerBeanDefinition(beanName, (BeanDefinition)BeanDefinitionBuilder.genericBeanDefinition(clazz).getBeanDefinition());
            }
            catch (Exception exception) {}
        }
        this.registerMongoTemplateBeanIfAvailable(context);
        this.registerJdbcTemplateBeanIfAvailable(context);
        return context;
    }

    private void registerMongoTemplateBeanIfAvailable(GenericApplicationContext context) {
        try {
            Class<?> mongoTemplateClass = Class.forName("org.springframework.data.mongodb.core.MongoTemplate", false, this.jarClassLoader.getClassLoader());
            UniversalMongoInterceptor interceptorFactory = new UniversalMongoInterceptor(this.queryInterceptor, this.realClient, this.realDbName);
            Class<?> proxyClass = interceptorFactory.buildProxyClass(mongoTemplateClass);
            context.registerBeanDefinition("mongoTemplate", (BeanDefinition)BeanDefinitionBuilder.genericBeanDefinition(proxyClass).getBeanDefinition());
        }
        catch (Exception exception) {
            // empty catch block
        }
    }

    private void registerJdbcTemplateBeanIfAvailable(GenericApplicationContext context) {
        try {
            Class<?> jdbcTemplateClass = Class.forName("org.springframework.jdbc.core.JdbcTemplate", false, this.jarClassLoader.getClassLoader());
            context.registerBeanDefinition("jdbcTemplate", (BeanDefinition)BeanDefinitionBuilder.genericBeanDefinition(jdbcTemplateClass).getBeanDefinition());
        }
        catch (Exception exception) {
            // empty catch block
        }
    }

    private boolean isServiceOrRepository(String stereotype) {
        if (stereotype == null) {
            return false;
        }
        String upper = stereotype.toUpperCase();
        return upper.contains("SERVICE") || upper.contains("REPOSITORY") || upper.contains("DAO");
    }

    private String simpleName(String fqn) {
        if (fqn == null) {
            return "unknown";
        }
        int dot = fqn.lastIndexOf(46);
        return dot >= 0 ? fqn.substring(dot + 1) : fqn;
    }
}

