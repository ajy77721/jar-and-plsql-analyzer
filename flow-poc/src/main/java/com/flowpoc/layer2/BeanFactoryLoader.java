package com.flowpoc.layer2;

import com.jaranalyzer.model.ClassInfo;
import com.mongodb.client.MongoClient;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.context.support.GenericApplicationContext;

import java.util.List;

public class BeanFactoryLoader {

    private final JarClassLoader             jarClassLoader;
    private final List<ClassInfo>            classInfos;
    private final CollectingQueryInterceptor queryInterceptor;
    private final MongoClient                realClient;
    private final String                     realDbName;

    /** Minimal constructor — no real-DB pre-seeding for writes. */
    public BeanFactoryLoader(JarClassLoader jarClassLoader,
                             List<ClassInfo> classInfos,
                             CollectingQueryInterceptor queryInterceptor) {
        this(jarClassLoader, classInfos, queryInterceptor, null, null);
    }

    /** Full constructor — realClient/realDbName enable fetch-before-mutate pre-seeding. */
    public BeanFactoryLoader(JarClassLoader jarClassLoader,
                             List<ClassInfo> classInfos,
                             CollectingQueryInterceptor queryInterceptor,
                             MongoClient realClient, String realDbName) {
        this.jarClassLoader   = jarClassLoader;
        this.classInfos       = classInfos;
        this.queryInterceptor = queryInterceptor;
        this.realClient       = realClient;
        this.realDbName       = realDbName;
    }

    public GenericApplicationContext buildContext() {
        GenericApplicationContext context = new GenericApplicationContext();

        for (ClassInfo ci : classInfos) {
            String stereotype = ci.getStereotype();
            if (!isServiceOrRepository(stereotype)) continue;

            try {
                Class<?> clazz = jarClassLoader.load(ci.getFullyQualifiedName());
                String beanName = ci.getSimpleName() != null
                        ? ci.getSimpleName()
                        : simpleName(ci.getFullyQualifiedName());
                context.registerBeanDefinition(beanName,
                        BeanDefinitionBuilder.genericBeanDefinition(clazz)
                                .getBeanDefinition());
            } catch (Exception ignored) {
            }
        }

        registerMongoTemplateBeanIfAvailable(context);
        registerJdbcTemplateBeanIfAvailable(context);

        return context;
    }

    private void registerMongoTemplateBeanIfAvailable(GenericApplicationContext context) {
        try {
            Class<?> mongoTemplateClass = Class.forName(
                    "org.springframework.data.mongodb.core.MongoTemplate",
                    false, jarClassLoader.getClassLoader());
            UniversalMongoInterceptor interceptorFactory =
                    new UniversalMongoInterceptor(queryInterceptor, realClient, realDbName);
            Class<?> proxyClass = interceptorFactory.buildProxyClass(mongoTemplateClass);
            context.registerBeanDefinition("mongoTemplate",
                    BeanDefinitionBuilder.genericBeanDefinition(proxyClass)
                            .getBeanDefinition());
        } catch (Exception ignored) {
        }
    }

    private void registerJdbcTemplateBeanIfAvailable(GenericApplicationContext context) {
        try {
            Class<?> jdbcTemplateClass = Class.forName(
                    "org.springframework.jdbc.core.JdbcTemplate",
                    false, jarClassLoader.getClassLoader());
            context.registerBeanDefinition("jdbcTemplate",
                    BeanDefinitionBuilder.genericBeanDefinition(jdbcTemplateClass)
                            .getBeanDefinition());
        } catch (Exception ignored) {
        }
    }

    private boolean isServiceOrRepository(String stereotype) {
        if (stereotype == null) return false;
        String upper = stereotype.toUpperCase();
        return upper.contains("SERVICE") || upper.contains("REPOSITORY")
                || upper.contains("DAO");
    }

    private String simpleName(String fqn) {
        if (fqn == null) return "unknown";
        int dot = fqn.lastIndexOf('.');
        return dot >= 0 ? fqn.substring(dot + 1) : fqn;
    }
}
