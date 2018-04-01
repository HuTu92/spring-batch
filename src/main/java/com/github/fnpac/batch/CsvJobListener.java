package com.github.fnpac.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.util.Date;

/**
 * Created by liuchunlong on 2018/3/29.
 */
public class CsvJobListener implements JobExecutionListener {

    private static final Logger logger = LoggerFactory.getLogger(CsvJobListener.class);

    @Override
    public void beforeJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        String jobConfigurationName = jobExecution.getJobConfigurationName();
        logger.info("Job[" + jobName + "/" + jobConfigurationName + "] start!~");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        String jobConfigurationName = jobExecution.getJobConfigurationName();
        Date startTime = jobExecution.getStartTime();
        Date endTime = jobExecution.getEndTime();
        logger.info("Job[" + jobName + "/" + jobConfigurationName + "] end! Spend times: " + (endTime.getTime() - startTime.getTime()) + "ms");
    }
}
