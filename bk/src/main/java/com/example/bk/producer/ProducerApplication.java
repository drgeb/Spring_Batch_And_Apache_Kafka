package com.example.bk.producer;

import com.example.bk.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.atomic.AtomicLong;

@EnableBatchProcessing
@SpringBootApplication
public class ProducerApplication {
    public static void main(String args[]) {
        SpringBootApplication.run(ProducerApplication.class, args);
    }

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final KafkaTemplate template;

    @Bean
    Job job() {
        return this.jobBuilderFactory
                .get("job")
                .start(start())
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    KafkaItemWriter<Long, Customer> kafkaItemWrite() {
        return new KafkaItemWriterBuilder<>()
                .kafkaTemplate(template)
                .itemKeyMapper(Customer::getId)
                .build();
    }
    @Bean
    Step start() {
        var id = new AtomicLong();
        var reader = new ItemReader<Customer>() {

            @Override
            public Customer read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                if(id.incrementAndGet() < 10_000)
                    return new Customer(id.get(), Math.random() > .5 ? "Jane" : "Jose");
                return null;
            }
        };

        return this.stepBuilderFactory
                .get("s1")
                .<Customer, Customer> chunk(10)
                .reader(reader)
                .writer(kafkaItemWriter())
                .build();
    }
}

