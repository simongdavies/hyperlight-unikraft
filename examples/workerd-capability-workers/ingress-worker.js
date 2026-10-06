export default {
  scheduled(controller) {
    if (controller.cron !== "0 0 * * *") {
      throw new Error(`unexpected cron ${controller.cron}`);
    }
  },
  queue(batch) {
    batch.messages[0].ack();
    batch.messages[1].retry({ delaySeconds: 7 });
  },
};
