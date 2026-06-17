package lnk.dong.gateway.filter.orderFillter;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
public class ThirdGlobalFilter implements GlobalFilter, Ordered {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        System.out.println("======= SecondGlobalFilter - PRE (order=10) =======");

        // 读取第一个过滤器存入的属性
        Long firstTime = exchange.getAttribute("first-filter-time");
        System.out.println("读取到 FirstFilter 存入的时间: " + firstTime);

        return chain.filter(exchange)
                .doFinally(signalType -> System.out.println("======= SecondGlobalFilter - POST (order=10) ======="));
    }

    //doFinally 的执行顺序始终是：后注册的先执行
    @Override
    public int getOrder() {
        return 10;   // 数值大，PRE阶段后执行；POST阶段先执行
    }
}