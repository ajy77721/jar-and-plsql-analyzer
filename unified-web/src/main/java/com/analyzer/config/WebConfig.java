package com.analyzer.config;

import com.analyzer.auth.ApiKeyAuthInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final ApiKeyAuthInterceptor authInterceptor;

    public WebConfig(ApiKeyAuthInterceptor authInterceptor) {
        this.authInterceptor = authInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // Auth disabled: API-key login gate removed for local use.
        // Claude AI features still use local `claude login` CLI credentials.
        // registry.addInterceptor(authInterceptor);
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        // /login is kept only so an old bookmark lands on an explanation rather than a 404.
        // The page no longer presents a sign-in form: auth is disabled (the interceptor below is
        // not registered), the form POSTed to /api/auth/login which does not exist, and it asked
        // for a live Anthropic API key to send nowhere.
        registry.addViewController("/login").setViewName("forward:/login.html");
        registry.addViewController("/jar").setViewName("redirect:/jar/");
        registry.addViewController("/jar/").setViewName("forward:/jar/index.html");
        registry.addViewController("/war").setViewName("redirect:/war/");
        registry.addViewController("/war/").setViewName("forward:/war/index.html");
        registry.addViewController("/plsql").setViewName("redirect:/plsql/");
        registry.addViewController("/plsql/").setViewName("forward:/plsql/index.html");
        registry.addViewController("/parser").setViewName("redirect:/parser/");
        registry.addViewController("/parser/").setViewName("forward:/parser/index.html");
        registry.addViewController("/conversion").setViewName("redirect:/conversion/");
        registry.addViewController("/conversion/").setViewName("forward:/conversion/index.html");
    }
}
