package com.analyzer.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/jar").setViewName("redirect:/jar/");
        registry.addViewController("/jar/").setViewName("forward:/jar/index.html");
        registry.addViewController("/war").setViewName("redirect:/war/");
        registry.addViewController("/war/").setViewName("forward:/war/index.html");
        registry.addViewController("/plsql").setViewName("redirect:/plsql/");
        registry.addViewController("/plsql/").setViewName("forward:/plsql/index.html");
        registry.addViewController("/parser").setViewName("redirect:/parser/");
        registry.addViewController("/parser/").setViewName("forward:/parser/index.html");
    }
}
