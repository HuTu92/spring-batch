package com.github.fnpac.config;

import com.github.fnpac.config.domain.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Created by liuchunlong on 2018/3/28.
 */
@Configuration
@EnableBatchProcessing
public class BatchConfig {

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
        jobRepositoryFactoryBean.setDatabaseType("mysql");
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
                .build();
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

                    chunkSize - 块大小（提交间隔）

                    Type parameters:
                    <I> - 作为输入处理的Item类型
                    <O> - 要输出的Item的类型

                    Returns:
                    SimpleStepBuilder
                 */
                .<Person, Person> chunk(65000)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public ItemReader<Person> reader() {
        return null;
    }

    @Bean
    public ItemProcessor<Person, Person> processor() {
        return null;
    }

    @Bean
    public ItemWriter<Person> writer() {
        return null;
    }
}
