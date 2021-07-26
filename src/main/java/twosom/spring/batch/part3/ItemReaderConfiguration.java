package twosom.spring.batch.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ItemReaderConfiguration {

    private final DataSource dataSource;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;



    @Bean
    public Job itemReaderJob() throws Exception {
        return this.jobBuilderFactory.get("itemReaderJob")
                .incrementer(new RunIdIncrementer())
                .start(this.customItemReaderStep())
                .next(this.locationCsvStep())
                .next(this.personCsvStep())
                .next(this.jdbcStep())
                .next(this.locationJdbcStep())
                .next(this.jpaStep())
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
    public Step jdbcStep() throws Exception {
        return stepBuilderFactory.get("jdbcStep")
                .<Person, Person>chunk(10)
                .reader(jdbcCursorItemReader())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Step jpaStep() throws Exception {
        return this.stepBuilderFactory.get("jpaStep")
                .<Location, Location>chunk(10)
                .reader(jpaCursorItemReader())
                .writer(items -> {
                    log.info(
                            items.stream()
                                    .map(Location::getKor)
                                    .collect(Collectors.joining("#"))
                    );
                })
                .build();
    }


    private JpaCursorItemReader<Location> jpaCursorItemReader() throws Exception {
        JpaCursorItemReader<Location> itemReader = new JpaCursorItemReaderBuilder<Location>()
                .name("jpaCursorItemReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select l from Location l")
                .build();

        itemReader.afterPropertiesSet();
        return itemReader;
    }


    private JdbcCursorItemReader<Person> jdbcCursorItemReader() throws Exception {
        JdbcCursorItemReader<Person> itemReader = new JdbcCursorItemReaderBuilder<Person>()
                .name("jdbcCursorItemReader")
                .dataSource(dataSource)
                .sql("select id, name, age, address from person")
                .rowMapper((rs, rowNum) -> Person.builder()
                        .id(rs.getInt("id"))
                        .name(rs.getString("name"))
                        .age(rs.getString("age"))
                        .address(rs.getString("address"))
                        .build())
                .build();

        itemReader.afterPropertiesSet();
        return itemReader;
    }

    private JdbcCursorItemReader<Location> locationJdbcCursorItemReader() throws Exception {
        JdbcCursorItemReader<Location> itemReader = new JdbcCursorItemReaderBuilder<Location>()
                .name("locationJdbcCursorItemReader")
                .dataSource(dataSource)
                .sql("select eng, kor, detail from location")
                .rowMapper((rs, rowNum) -> Location.builder()
                        .eng(rs.getString("eng"))
                        .kor(rs.getString("kor"))
                        .detail(rs.getString("detail"))
                        .build())
                .build();

        itemReader.afterPropertiesSet();
        return itemReader;
    }

    @Bean
    public Step locationJdbcStep() throws Exception {
        return this.stepBuilderFactory.get("locationJdbcStep")
                .<Location, Location>chunk(10)
                .reader(locationJdbcCursorItemReader())
                .writer(items -> {
                    log.info(
                            items.stream()
                                    .map(Location::getKor)
                                    .collect(Collectors.joining(","))
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
