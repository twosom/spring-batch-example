package twosom.spring.batch.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ItemReaderConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;


    @Bean
    public Job itemReaderJob() throws Exception {
        return this.jobBuilderFactory.get("itemReaderJob")
                .incrementer(new RunIdIncrementer())
                .start(this.customItemReaderStep())
                .next(this.locationCsvStep())
                .next(this.personCsvStep())
                .build();
    }

    @Bean
    public Step customItemReaderStep() {
        return this.stepBuilderFactory.get("customItemReaderStep")
                .<Person, Person>chunk(10)
                .reader(new CustomItemReader<>(getItems()))
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Step locationCsvStep() throws Exception {
        return this.stepBuilderFactory.get("csvStep")
                .<Location, Location>chunk(10)
                .reader(locationCsvItemReader())
                .writer(items -> {
                    log.info(items.stream()
                            .map(Location::getKor)
                            .collect(Collectors.joining(", "))
                    );
                })
                .build();
    }

    @Bean
    public Step personCsvStep() throws Exception {
        return this.stepBuilderFactory.get("personCsvStep")
                .<Person, Person>chunk(10)
                .reader(personCsvItemReader())
                .writer(itemWriter())
                .build();
    }

    private FlatFileItemReader<Person> personCsvItemReader() throws Exception {
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "name", "age", "address");
        lineMapper.setLineTokenizer(tokenizer);

        lineMapper.setFieldSetMapper(fieldSet -> Person.builder()
                .id(fieldSet.readInt("id"))
                .name(fieldSet.readString("name"))
                .age(fieldSet.readString("age"))
                .address(fieldSet.readString("address"))
                .build());

        FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
                .name("personCsvItemReader")
                .resource(new ClassPathResource("test.csv"))
                .encoding(StandardCharsets.UTF_8.name())
                .lineMapper(lineMapper)
                .linesToSkip(1)
                .build();
        itemReader.afterPropertiesSet();
        return itemReader;
    }


    private FlatFileItemReader<Location> locationCsvItemReader() throws Exception {
        DefaultLineMapper<Location> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("eng", "kor", "detail");
        lineMapper.setLineTokenizer(tokenizer);

        lineMapper.setFieldSetMapper(fieldSet -> Location.builder()
                .eng(fieldSet.readString("eng"))
                .kor(fieldSet.readString("kor"))
                .detail(fieldSet.readString("detail"))
                .build());

        FlatFileItemReader<Location> itemReader = new FlatFileItemReaderBuilder<Location>()
                .name("locationCsvItemReader")
                .resource(new ClassPathResource("zones_kr.csv"))
                .encoding(StandardCharsets.UTF_8.name())
                .linesToSkip(0)
                .lineMapper(lineMapper)
                .build();

        itemReader.afterPropertiesSet();
        return itemReader;
    }


    private ItemWriter<Person> itemWriter() {
        return items -> log.info(
                items.stream()
                        .map(Person::getName)
                        .collect(Collectors.joining(", "))
        );
    }

    private List<Person> getItems() {
        return IntStream.range(0, 10)
                .mapToObj(i -> Person.builder()
                                    .id(i + 1)
                                    .name("test name" + i)
                                    .age("test age")
                                    .address("test address")
                                    .build())
                .collect(Collectors.toList());
    }

}
