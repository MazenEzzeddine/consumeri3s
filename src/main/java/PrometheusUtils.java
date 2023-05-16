import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class PrometheusUtils {
    public static PrometheusMeterRegistry prometheusRegistry;
    public static TimeMeasure latencygaugemeasure;
    public static Gauge latencygauge;


    public static TimeMeasure maxlatencygaugemeasure;
    public static Gauge maxlatencygauge;




    public static void initPrometheus() {
        prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
            server.createContext("/prometheus", httpExchange -> {
                String response = prometheusRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });
            new Thread(server::start).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        latencygaugemeasure = new TimeMeasure(0.0);
        latencygauge = Gauge.builder("latencygauge",  latencygaugemeasure, TimeMeasure::getDuration)
                .register(prometheusRegistry);


        maxlatencygaugemeasure = new TimeMeasure(0.0);
        maxlatencygauge = Gauge.builder("maxlatencygauge",  maxlatencygaugemeasure,
                        TimeMeasure::getDuration)
                .register(prometheusRegistry);


    }
}