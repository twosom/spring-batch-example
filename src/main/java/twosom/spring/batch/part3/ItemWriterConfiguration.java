package twosom.spring.batch.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ItemWriterConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;


    @Bean
    public Job itemWriterJob() throws Exception {
        return this.jobBuilderFactory.get("itemWriterJob")
                .incrementer(new RunIdIncrementer())
                .start(this.csvItemWriterStep())
                .build();
    }

    @Bean
    public Step csvItemWriterStep() throws Exception {
        return this.stepBuilderFactory.get("csvItemWriterStep")
                .<Person, Person>chunk(10)
                .reader(itemReader())
                .writer(csvFileItemWriter())
                .build();
    }

    private ItemWriter<Person> csvFileItemWriter() throws Exception {
        BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"id", "name", "age", "address"});

        DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        FlatFileItemWriter<Person> itemWriter = new FlatFileItemWriterBuilder<Person>()
                .name("csvFileItemWriter")
                .encoding(StandardCharsets.UTF_8.name())
                .resource(new FileSystemResource("output/test-output.csv"))
                .lineAggregator(lineAggregator)
                .headerCallback(writer -> writer.write("id, 이름, 나이, 거주지"))
                .footerCallback(writer -> writer.write("-------------------\n"))
                .append(true)
                .build();

        itemWriter.afterPropertiesSet();
        return itemWriter;
    }

    private ItemReader<Person> itemReader() {
        return new CustomItemReader<>(getItems());
    }

    private List<Person> getItems() {
        return IntStream
                .range(0, 100)
                .mapToObj(i -> Person.builder()
                        .id(i + 1)
                        .name("test name " + i)
                        .age("test age")
                        .address("test address")
                        .build())
                .collect(Collectors.toList());
    }

}
