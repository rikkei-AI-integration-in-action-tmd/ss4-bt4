package com.rikkei.stream.controller;

import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.Objects;

@RestController
@RequestMapping("/api/v1/incident")
public class IncidentStreamController {

    private final ChatModel chatModel;

    public IncidentStreamController(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> streamIncidentAnalysis(
            @RequestParam String rawMessage,
            @RequestParam(defaultValue = "0.5") Double temp,
            @RequestParam(defaultValue = "1000") Integer maxTokens,
            ServerHttpResponse response
    ) {
        response.getHeaders().set("X-Accel-Buffering", "no");
        response.getHeaders().set("Cache-Control", "no-cache");

        OpenAiChatOptions dynamicOptions = OpenAiChatOptions.builder()
                .withTemperature(temp)
                .withMaxTokens(maxTokens)
                .build();

        String systemInstruction = "Bạn là trợ lý điều phối logistics. Hãy phân tích nhanh và chi tiết sự cố sau đây:";
        String fullPrompt = systemInstruction + "\n" + rawMessage;

        Prompt prompt = new Prompt(fullPrompt, dynamicOptions);

        return chatModel.stream(prompt)
                .filter(chatResponse -> chatResponse.getResult() != null
                        && chatResponse.getResult().getOutput() != null
                        && chatResponse.getResult().getOutput().getContent() != null)
                .map(chatResponse -> chatResponse.getResult().getOutput().getContent())
                .filter(text -> !text.isEmpty());
    }
}
