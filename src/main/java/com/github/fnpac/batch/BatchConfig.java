package com.github.fnpac.batch;

import com.github.fnpac.domain.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.validator.Validator;
import org.springframework.batch.support.DatabaseType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Created by liuchunlong on 2018/3/28.
 */
@Configuration
@EnableBatchProcessing
public class BatchConfig {

    /**
     * 用于step作用域的beans的一个方便的注解，其指定了默认的代理模式（proxyMode），因此不必在每个bean定义中再明确指定。
     * <p>
     * 在任何需要从Step上下文中注入@Value的@Bean，以及任何需要通过stepExecution共享生命周期的bean（例如，ItemStream的实现类）上使用它。
     * </p>
     * <p>
     * <pre class="code">
     * &#064;Bean
     * &#064;StepScope
     * protected Callable&lt;String&gt; value(@Value(&quot;#{stepExecution.stepName}&quot;)
     * final String value) {
     * return new SimpleCallable(value);
     * }
     * </pre>
     * <p>
     * 将@Bean标记为@StepScope相当于将其标记为@Scope(value="step", proxyMode=TARGET_CLASS)
     * <p>
     * 类似的，还有@JobScope注解。
     *
     * @param pathToFile
     * @return
     */
    @Bean
    // Since you put the reader in StepScope, the bean return type should be the implementing type FlatFileItemReader
    // otherwise，will issues：“ReaderNotOpenException: Reader must be open before it can be read”
    @StepScope
    public FlatFileItemReader<Person> reader(@Value("#{jobParameters['input.file.name']}") String pathToFile) {
        /*
            可重用的ItemReader，用于从输入setResource(Resource)中读取行。
            行（Line）由setRecordSeparatorPolicy(RecordSeparatorPolicy)定义，并使用setLineMapper(LineMapper)映射到Item。
            如果在item映射期间抛出异常，则将其重新抛出为FlatFileParseException，添加有关有问题的行及其行号的信息。
         */
        FlatFileItemReader<Person> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource(pathToFile));
        /*
            设置决定是否为ExecutionContext保存内部数据的标志。
            仅当您不想保存此Stream中的任何状态，并且您不需要它可以重新启动，将其切换为false。
            如果reader在并发环境中使用，请始终将其设置为false。

            等价于 SynchronizedItemStreamReader
         */
//        reader.setSaveState(false);
        reader.setLineMapper(new DefaultLineMapper<Person>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {{
//                    setDelimiter(",");
                    setNames(new String[]{"name", "age", "nation", "address"});
                }});
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                    setTargetType(Person.class);
                }});
            }
        });

        /*
            这是一个带有同步的ItemReader.read()方法的简单ItemStreamReader装饰器 - 它使非线程安全的ItemReader成为线程安全的。
            但是，如果重新处理某个item时出现问题，则使用此操作将导致作业无法重新启动。
            这里有一些关于这个类背后动机的链接：
            -  http://projects.spring.io/spring-batch/faq.html#threading-reader
            -  http://stackoverflow.com/a/20002493/2910265}
         */
//        SynchronizedItemStreamReader<Person> syncReader = new SynchronizedItemStreamReader<>();
//        syncReader.setDelegate(reader);
//        return syncReader;
        return reader;
    }

    @SuppressWarnings("unchecked")
    @Bean
    public ItemProcessor<Person, Person> processor() {
        CsvItemProcessor processor = new CsvItemProcessor();
        processor.setValidator(csvBeanValidator());
        return processor;
    }

    @Bean
    public Validator csvBeanValidator() {
        return new CsvBeanValidator<Person>();
    }

    @Bean
    public ItemWriter<Person> writer(DataSource dataSource) {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();

        // interface ItemSqlParameterSourceProvider<T>
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());

        String sql = "insert into person (name, age, nation, address) " +
                "values (:name, :age, :nation, :address)";
        writer.setSql(sql);
        writer.setDataSource(dataSource);
        return writer;
    }

    /**
     * JobRepository
     *
     * @param dataSource
     * @param transactionManager
     * @return
     * @throws Exception
     */
    @Bean
    public JobRepository jobRepository(
            DataSource dataSource,
            PlatformTransactionManager transactionManager) throws Exception {
        JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
        jobRepositoryFactoryBean.setDataSource(dataSource);
        jobRepositoryFactoryBean.setDatabaseType(DatabaseType.MYSQL.getProductName());
        jobRepositoryFactoryBean.setTransactionManager(transactionManager);
        return jobRepositoryFactoryBean.getObject();
    }

    /**
     * JobLauncher
     *
     * @param dataSource
     * @param transactionManager
     * @return
     * @throws Exception
     */
    @Bean
    public SimpleJobLauncher jobLauncher(
            DataSource dataSource,
            PlatformTransactionManager transactionManager) throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();// JobLauncher's only implement
        jobLauncher.setJobRepository(jobRepository(dataSource, transactionManager));
        return jobLauncher;
    }

    /**
     * Job
     *
     * @param jobBuilderFactory
     * @param step
     * @return
     */
    @Bean
    public Job importJob(JobBuilderFactory jobBuilderFactory, Step step) {
        // 创建JobBuilder并初始化它的jobRepository
        // 注意，如果构建器用于@Bean定义的创建，则job名称和bean名称可能会有所不同
        return jobBuilderFactory.get("importJob")
                // The name of the run id in the job parameters.  Defaults to "run.id".
                // Increment the run.id parameter (starting with 1).
                .incrementer(new RunIdIncrementer())
                // 创建一个将执行一个步骤或一系列步骤的新JobFlowBuilder
                /*
                    =================================================
                    JobBuilder extends JobBuilderHelper<JobBuilder>
                    =================================================
                    JobFlowBuilder extends FlowBuilder<FlowJobBuilder>

                    FlowJobBuilder extends JobBuilderHelper<FlowJobBuilder>
                    =================================================

                    flow方法只要执行如下逻辑：
                        由 JobBuilder 通过 flow()方法 构造 FlowJobBuilder（JobBuilder作为parent传入构造函数），
                        FlowJobBuilder 通过 start() 方法构造 JobFlowBuilder（FlowJobBuilder 作为parent传入构造函数）
                        最后返回 JobFlowBuilder
                 */
                .flow(step)
                .end() // return FlowJobBuilder
                .listener(csvJobListener())
                .build();
    }

//    @Bean
//    public JobParametersIncrementer runIdIncrementer() {
//        return new RunIdIncrementer();
//    }

    @Bean
    public CsvJobListener csvJobListener() {
        return new CsvJobListener();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory,
                     ItemReader<Person> reader,
                     ItemWriter<Person> writer,
                     ItemProcessor<Person, Person> processor) {
        // 创建StepBuilder并初始化它的jobRepository和transactionManager
        // 注意，如果构建器用于@Bean定义的创建，则step名称和bean名称可能会有所不同
        return stepBuilderFactory.get("step")
                /*
                    构建一个在指定大小的块(chunks)中处理Item的Step。要将该Step扩展为容错，请在builder上调用SimpleStepBuilder.faultTolerant()方法。在大多数情况下，您需要参数化您对此方法的调用，以保证类型安全性，例如，

                    new StepBuilder("step1").<Order, Ledger> chunk(100).reader(new OrderReader()).writer(new LedgerWriter())

                    Parameters:

                    chunkSize - 块大小（提交间隔），即一次提交多少记录

                    Type parameters:
                    <I> - 作为输入处理的Item类型
                    <O> - 要输出的Item的类型

                    Returns:
                    SimpleStepBuilder
                 */
                .<Person, Person>chunk(65000)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }
}
