package twosom.spring.batch.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ChunkProcessingConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;


    @Bean
    public Job chunkProcessingJob() {
        return jobBuilderFactory.get("chunkProcessingJob")
                .incrementer(new RunIdIncrementer())
                .start(this.taskBaseStep())
                .next(chunkBaseStep())
                .build();
    }

    @Bean
    public Step chunkBaseStep() {
        return stepBuilderFactory.get("chunkBaseStep")
                .<String, String>chunk(10)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    private ItemReader<String> itemReader() {
        return new ListItemReader<>(getItems());
    }

    private ItemProcessor<String, String> itemProcessor() {
        return item -> item + ", Spring Batch";
    }

    private ItemWriter<String> itemWriter() {
        return items -> log.info("chunk item size : {}", items.size());
    }


    @Bean
    public Step taskBaseStep() {
        return stepBuilderFactory.get("taskBaseStep")
                .tasklet(this.tasklet())
                .build();
    }




    private Tasklet tasklet() {
        List<String> items = getItems();
        return (contribution, chunkContext) -> {
            StepExecution stepExecution = contribution.getStepExecution();

            int chunkSize = 10;
            int fromIndex = stepExecution.getReadCount();
            int toIndex = fromIndex + chunkSize;

            if (fromIndex >= items.size()) {
                return RepeatStatus.FINISHED;
            }

            List<String> subList = items.subList(fromIndex, toIndex);
            log.info("task item size : {}", subList.size());

            stepExecution.setReadCount(toIndex);
            return RepeatStatus.CONTINUABLE;
        };
    }

    private List<String> getItems() {
        return IntStream
                .range(0, 100)
                .mapToObj(i -> i + " Hello")
                .collect(Collectors.toList());
    }
}
