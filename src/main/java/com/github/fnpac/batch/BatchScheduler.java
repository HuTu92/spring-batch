package com.github.fnpac.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.UUID;

/**
 * Created by 刘春龙 on 2018/3/31.
 */
@Configuration
@EnableScheduling
public class BatchScheduler implements InitializingBean {

    private final RunIdIncrementer runIdIncrementer = new RunIdIncrementer();

    private final JobLauncher jobLauncher;
    private final Job importJob;

    @Autowired
    public BatchScheduler(JobLauncher jobLauncher, Job importJob) {
        this.jobLauncher = jobLauncher;
        this.importJob = importJob;
    }

//    @Scheduled(fixedDelay = 1000)
    public void batchRun() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        String path = "people.csv";

        JobParameters jobParameters = new JobParametersBuilder()
                /*
                    确保每次任务执行jobParameters不一样，否则重复执行任务时，会报任务已存在错误。

                        错误如下：
                            org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException: A job instance already exists and is complete for parameters={input.file.name=people.csv}.  If you want to run this job again, change the parameters.

                    有以下两种方式解决：
                 */
                // 1. 设置一个唯一性参数，key任意，value确保唯一性
                .addString("uid", UUID.randomUUID().toString() + "@" + System.currentTimeMillis())
                // 2. 通过runIdIncrementer，但是当项目重启时，该方法则还是会不可避免的出现“任务已存在错误”，因为jobParameters会重置，"run.id"又从1开始计数
//                .addLong("run.id", 0L) // 可以省略，runIdIncrementer 默认从0开始
                .addString("input.file.name", path)
                .toJobParameters();
//        for (int i = 0; i < 3; i++) {// 多次执行job
            // 2. 通过runIdIncrementer
//            jobParameters = runIdIncrementer.getNext(jobParameters);
            jobLauncher.run(importJob, jobParameters);
//        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        batchRun();
    }
}
