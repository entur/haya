package org.entur.haya;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;

@SpringBootApplication
@EnableTask
public class HayaApplication {

    public static void main(String[] args) {
        SpringApplication.run(HayaApplication.class, args);
    }

}
