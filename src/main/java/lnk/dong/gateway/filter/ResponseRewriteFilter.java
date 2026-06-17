package lnk.dong.gateway.filter;


import org.reactivestreams.Publisher;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

@Component
public class ResponseRewriteFilter implements GlobalFilter, Ordered {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpResponse originalResponse = exchange.getResponse();
        DataBufferFactory bufferFactory = originalResponse.bufferFactory();

        // 包装原始响应
        ServerHttpResponseDecorator decoratedResponse = new ServerHttpResponseDecorator(originalResponse) {
            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                // 只处理 JSON 类型的响应
                MediaType contentType = originalResponse.getHeaders().getContentType();
                if (contentType == null || !contentType.includes(MediaType.APPLICATION_JSON)) {
                    return super.writeWith(body);
                }

                // 从 DataBuffer 中读取响应体内容
                Flux<? extends DataBuffer> bodyFlux = Flux.from(body);
                return super.writeWith(bodyFlux.buffer().map(dataBuffers -> {
                    // 合并所有 DataBuffer 为字节数组
                    int totalLength = dataBuffers.stream().mapToInt(DataBuffer::readableByteCount).sum();
                    byte[] combinedBytes = new byte[totalLength];
                    int offset = 0;
                    for (DataBuffer buffer : dataBuffers) {
                        int length = buffer.readableByteCount();
                        buffer.read(combinedBytes, offset, length);
                        offset += length;
                        // 释放 buffer，防止内存泄漏
                        DataBufferUtils.release(buffer);
                    }

                    // 转为原始响应字符串
                    String originalBody = new String(combinedBytes, StandardCharsets.UTF_8);
                    System.out.println("【原始响应体】: " + originalBody);

                    // ===== 在这里进行响应体改造 =====
                    // 示例：将 JSON 中的 "phone": "13800138000" 替换为 "phone": "****"
                    String modifiedBody = originalBody.replaceAll("\"phone\":\"(\\d{11})\"", "\"phone\":\"****\"");

                    // 示例：统一包装响应（如果原始不是标准格式）
                    // String wrappedBody = "{\"code\":0,\"data\":" + originalBody + "}";

                    System.out.println("【修改后响应体】: " + modifiedBody);

                    // 重新设置 Content-Length（因为内容可能变长或变短）
                    byte[] modifiedBytes = modifiedBody.getBytes(StandardCharsets.UTF_8);
                    originalResponse.getHeaders().setContentLength(modifiedBytes.length);

                    // 返回新的 DataBuffer
                    return bufferFactory.wrap(modifiedBytes);
                }));
            }

            @Override
            public Mono<Void> writeAndFlushWith(Publisher<? extends Publisher<? extends DataBuffer>> body) {
                return writeWith(Flux.from(body).flatMapSequential(p -> p));
            }
        };

        // 用装饰后的响应替换原始响应，继续执行
        return chain.filter(exchange.mutate().response(decoratedResponse).build());
    }

    @Override
    public int getOrder() {
        return -2;  // 保证在响应返回时，最先拦截到原始响应体
    }
}