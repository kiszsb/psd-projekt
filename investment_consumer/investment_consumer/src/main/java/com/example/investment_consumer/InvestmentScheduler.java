package com.example.investment_consumer;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Component
@EnableScheduling
@RequiredArgsConstructor
public class InvestmentScheduler {

    private final InvestmentProducer investmentController;

    private final Random random = new Random();

    @Scheduled(fixedDelay = 1)
    public void executeTask() throws InterruptedException {
        waitRandomTimeInterval(200, 1000);
        investmentController.publishReturnOnInvestment();
    }

    private void waitRandomTimeInterval(int min, int max) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(random.nextInt(max) + min);
    }
}
