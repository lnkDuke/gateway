package lnk.dong.gateway.config;


import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import reactor.core.publisher.Mono;

@Configuration
public class GatewayConfig {

    @Bean
    public RouteLocator customRouteLocator(RouteLocatorBuilder builder) {
        return builder.routes()
                .route("test_route", r -> r
                        .path("/test/**")
                        .filters(f -> f
                                // 0. 路径重写：去掉 /test 前缀再转发（否则 httpbin.org 返回 404）
                                .rewritePath("/test/(?<segment>.*)", "/${segment}")
                                // 1. 内置过滤器：添加请求头
                                .addRequestHeader("X-From-Gateway", "Hello")
                                // 2. 内置过滤器：添加响应头
                                .addResponseHeader("X-Powered-By", "Spring-Gateway")
                                // 3. 自定义过滤器：打印请求日志（局部过滤器）
                                .filter((exchange, chain) -> {
                                    ServerHttpRequest request = exchange.getRequest();
                                    ServerHttpResponse response = exchange.getResponse();

                                    // 打印请求信息
                                    System.out.println("========================================");
                                    System.out.println("【Gateway日志】请求路径: " + request.getPath().value());
                                    System.out.println("【Gateway日志】请求方法: " + request.getMethod());
                                    System.out.println("【Gateway日志】请求头: " + request.getHeaders());
                                    System.out.println("【Gateway日志】请求参数: " + request.getQueryParams());
                                    System.out.println("========================================");

                                    // 继续执行后续过滤器链
                                    Mono<Void> result = chain.filter(exchange);

                                    // 注意：这里打印响应信息需要特殊处理（响应体是流式的）
                                    // 后续进阶会讲如何打印响应体，现在先打印响应状态码
                                    System.out.println("【Gateway日志】响应状态码: " + response.getStatusCode());
                                    return result;
                                })
                        )
                        // 目标URI：使用公网测试API（返回请求的详细信息）
                        .uri("http://httpbin.org:80")
                )
                .build();
    }
}