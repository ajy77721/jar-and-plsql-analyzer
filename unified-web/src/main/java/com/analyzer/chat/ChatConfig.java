package com.analyzer.chat;

import com.plsqlanalyzer.web.config.ConfigDirService;
import com.plsqlanalyzer.web.service.ClaudeProcessRunner;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ChatConfig {

    @Bean
    public ChatService chatService(
            @Qualifier("plsqlClaudeProcessRunner") ClaudeProcessRunner processRunner,
            ConfigDirService dirs) {
        return new ChatService(processRunner, dirs.getChatbotDataDir().toString());
    }
}
