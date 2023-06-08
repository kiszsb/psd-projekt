package com.example.investment_consumer;

import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class ROIGenerator {

    private static final double RANGE = 0.1;

    private final Random random = new Random();

    public double generate() {
        double mean = Double.valueOf(0.004);
        double variance = Double.valueOf(0.009);
        double standardDeviation = Math.sqrt(variance);
        return generate(mean, standardDeviation);
    }

    private double generate(double mean, double standardDeviation) {
        double randomNumber;
        do {
            randomNumber = random.nextGaussian() * standardDeviation + mean;
        } while (randomNumber < -RANGE || randomNumber > RANGE);
        return randomNumber;
    }
}
