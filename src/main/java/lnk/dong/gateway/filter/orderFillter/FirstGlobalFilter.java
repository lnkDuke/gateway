package lnk.dong.gateway.filter.orderFillter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
public class FirstGlobalFilter implements GlobalFilter, Ordered {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        System.out.println("======= FirstGlobalFilter - PRE (order=1000) =======");

        // 在请求属性中存入标记，供后续过滤器读取
        exchange.getAttributes().put("first-filter-time", System.currentTimeMillis());

        // 继续执行
        return chain.filter(exchange)
                .doFinally(signalType -> System.out.println("======= FirstGlobalFilter - POST (order=1000) ======="));
    }

    @Override
    public int getOrder() {
        return 1000;  // 数值越小，PRE阶段越先执行；POST阶段越后执行
    }
}