// A standalone process, separate from the API and workers — same "separate
// entry point" principle as worker.js. This is a stand-in for a real
// analytics/notification service: it subscribes to job-events and reacts,
// completely decoupled from the worker code that published them. If this
// process is down, events just accumulate in Kafka (up to retention) —
// job processing is entirely unaffected.
import { Kafka } from "kafkajs";
import { env } from "../../config/env.js";
import { JOB_EVENTS_TOPIC } from "../producer.js";

const kafka = new Kafka({
  clientId: "analytics-consumer",
  brokers: [env.kafkaBroker],
});

// Consumer group id matters: multiple instances of THIS SAME consumer
// sharing "analytics-group" would split the topic's partitions between
// them (horizontal scaling of consumption). A different service (e.g. a
// future notifications consumer) would use its own group id, so it gets
// its own independent copy of every event — that's what lets many
// unrelated consumers each see the full event stream.
const consumer = kafka.consumer({ groupId: "analytics-group" });

async function run() {
  await consumer.connect();
  console.log("[analytics-consumer] connected");

  await consumer.subscribe({ topic: JOB_EVENTS_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      console.log(`[analytics-consumer] ${event.eventType} — job ${event.jobId}`, event);
      // A real implementation would write to an analytics collection/warehouse
      // here. For this project, logging is enough to demonstrate the pattern.
    },
  });
}

run().catch((err) => {
  console.error("[analytics-consumer] fatal error:", err);
  process.exit(1);
});
