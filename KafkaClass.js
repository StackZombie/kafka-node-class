class KafkaClass {
	constructor(host) {
		this._host = host;
		this._kafka = require("kafka-node");
	}

	topicAvailability(topic) {
		return new Promise((resolve, reject) => {
			var client = new this._kafka.KafkaClient({ kafkaHost: this._host });
			client.loadMetadataForTopics([topic], (err, resp) => {
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
			var client = new this._kafka.KafkaClient({ kafkaHost: this._host }),
				offset = new this._kafka.Offset(client);

			return new Promise((resolve, reject) => {
				offset.fetchLatestOffsets([topic], function (error, offsets) {
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
			var Producer = this._kafka.Producer,
				client = new this._kafka.KafkaClient({ kafkaHost: this._host }),
				producer = new Producer(client),
				payloads = [
					{
						topic: topic,
						messages: JSON.stringify(message),
						partition: partition == null ? 0 : partition
					}
				];


			producer.on("ready", function () {
				producer.send(payloads, function (err, data) {
					console.log("data:", data);
				});
			});

			producer.on("error", function (err) {
				console.log(err);
			});

		}
		catch (e) {
			console.log(e);
		}
	}

	getPayload(topic) {
		try {
			var Consumer = this._kafka.Consumer,
				client = new this._kafka.KafkaClient({ kafkaHost: this._host }),
				consumer = new Consumer(
					client,
					[
						{
							topic: topic,
							partition: 0
						}
					],
					{
						autoCommit: false
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
module.exports = { KafkaClass };