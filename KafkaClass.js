class KafkaClass {
	constructor() {

		this._kafka = require("kafka-node");
		this.client = new this._kafka.KafkaClient(process.env.KAFKA_HOST);
		this.offset = new this._kafka.offset(this.client);
		this.Producer = this._kafka.Producer;
		this.producer = new this.Producer(this.client);
		this.Consumer = this._kafka.Consumer;
		this.client = new this._kafka.KafkaClient(process.env.KAFKA_HOST);
	}

	topicAvailability(topic) {
		return new Promise((resolve, reject) => {

			this.client.loadMetadataForTopics([topic], (err, resp) => {
				if (resp) {
					return resolve(true);
				} else {
					console.log("Error", err);
					return reject(false);
				}
			});
		});


	}
	latestOffset(topic, partition) {

		try {

			return new Promise((resolve, reject) => {
				this.offset.fetchLatestOffsets([topic], function (error, offsets) {
					if (error) {
						return reject(error);
					}
					return resolve(offsets[topic][partition == null ? 0 : partition]);

				});
			});
		} catch (error) {
			console.log("Error", error);
		}
	}

	sendPayload(message, topic, partition) {
		try {
			console.log("producer Hit....");
			let payloads = [
				{
					topic: topic,
					messages: JSON.stringify(message),
					partition: partition == null ? 0 : partition
				}
			];


			this.producer.on("ready", function () {
				this.producer.send(payloads, function (err, data) {
					console.log("data:", data);
				});
			});

			this.producer.on("error", function (err) {
				console.log(err);
			});

		}
		catch (e) {
			console.log(e);
		}
	}

	getPayload(topic) {
		try {
			var consumer = new this.Consumer(
				this.client,
				[
					{
						topic: topic,
						partition: 0
					}
				],
				{
					autoCommit: true
				}
			);


			return new Promise((res, rej) => {
				consumer.on("message", async (data) => {
					console.log("Receive : ", data);
					return res(data);
				});
				consumer.on("error", function (err) {
					console.log("Consumer error: ", err);
					return rej(err);
				});
			});


		} catch (e) {
			console.log("error", e);
		}
	}

}

module.exports = new KafkaClass();