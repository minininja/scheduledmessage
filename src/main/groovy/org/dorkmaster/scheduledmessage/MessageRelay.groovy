package org.dorkmaster.scheduledmessage

import groovy.json.JsonOutput
import org.quartz.Job
import org.quartz.JobDataMap
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate

class MessageRelay implements Job {
    static Logger logger = LoggerFactory.getLogger(MessageRelay.class)

    @Autowired
    private KafkaTemplate<String,String> template

    @Override
    void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDataMap map = jobExecutionContext.getMergedJobDataMap()
        def topic = map.get("topic")
        def payload = map.get("payload")
        if (payload instanceof Map) {
            payload = JsonOutput.toJson(payload)

            logger.debug("Sending to ${topic} message ${payload}")
            template.send(topic, payload)
        }
        else {
            logger.warn("Job context does not contain a topic and payload ${map}")
        }
    }
}
