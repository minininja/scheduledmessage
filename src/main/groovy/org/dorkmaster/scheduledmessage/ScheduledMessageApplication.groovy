package org.dorkmaster.scheduledmessage

import ch.qos.logback.classic.Logger
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.quartz.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.quartz.SchedulerFactoryBean

import java.text.SimpleDateFormat

import static org.quartz.JobBuilder.newJob
import static org.quartz.TriggerBuilder.newTrigger

@SpringBootApplication
public class ScheduledMessageApplication implements CommandLineRunner {

	private static Logger logger = LoggerFactory.getLogger(ScheduledMessageApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(ScheduledMessageApplication, args)
	}

	protected SimpleDateFormat sdf() {
		new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
	}

	@Autowired
	private KafkaTemplate<String,String> template

	@Autowired
	private SchedulerFactoryBean scheduler

	@Override
	public void run(String... args) throws Exception {
		for (int i = 0; i < 20; i++) {
			def when = sdf().format(new Date(new Date().getTime() + 5 * 1000))
			String payload = JsonOutput.toJson([
					id: UUID.randomUUID().toString(),
					topic: 'test-topic',
					at: when,
					message: [
							test: 1,
							expectedArrival: when,
							sent: sdf().format(new Date())
					]
			])
			this.template.send("scheduled-message", payload)
			Thread.sleep(1000)
		}
	}

	private JobDataMap generateJobDataMap(def message) {
		JobDataMap map = new JobDataMap()
		map.put("topic", message.topic)
		map.put("payload", message.message)
		map
	}

	@KafkaListener(topics="scheduled-message")
	protected void listen(ConsumerRecord<?,?> cr) {
		def message = new JsonSlurper().parseText(cr.value().toString())
		if (message.id && message.topic && message.at && message.message) {
			def id = message.id
			def when = sdf().parse(message.at)

			SimpleTrigger trigger = newTrigger()
				.withIdentity(message.id, "scheduled-message")
				.usingJobData(generateJobDataMap(message))
				.startAt(when)
				.build()

			scheduler.scheduler.scheduleJob(newJob(MessageRelay.class).build(), trigger)
		}
	}
}
