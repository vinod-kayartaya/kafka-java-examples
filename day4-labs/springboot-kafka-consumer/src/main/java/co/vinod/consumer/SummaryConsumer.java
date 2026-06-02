package co.vinod.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SummaryConsumer {

    @KafkaListener(topics = { "city-payment-summary" })
    public void showCityPaymentSummary(String message) {
        System.out.println("::::::: Summary :::::: " + message);
    }

    @KafkaListener(topics = {  "high-value-payments" })
    public void showHighValuePaymentSummary(String message) {
        System.out.println("<<<<<< Summary >>>>>> " + message);
    }
}
