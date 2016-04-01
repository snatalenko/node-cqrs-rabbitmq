'use strict';

const amqp = require('amqplib');
const uuid = require('uuid');
const debug = require('debug')('cqrs:RabbitMqBus');
const reconnect = require('./reconnect');
const DEFAULT_PREFETCH = 100;

function descriptor(message) {
	if (!message) throw new TypeError('message argument required');
	if (!message.fields) throw new TypeError('message.fields argument required');
	if (!message.properties) throw new TypeError('message.properties argument required');

	if (message.properties.messageId) {
		return `${message.properties.type} (${message.properties.messageId})`;
	} else {
		return `${message.properties.type}-${message.fields.consumerTag}-${message.fields.deliveryTag}`;
	}
}

function publish(message, appId) {
	if (!message) throw new TypeError('message argument required');
	if (typeof message.type !== 'string' || !message.type.length) throw new TypeError('message.type argument must be a non-empty String');
	if (appId && typeof appId !== 'string') throw new TypeError('appId argument, when provided, must be a String');

	return channel => new Promise(function (resolve, reject) {

		const exchange = message.type;
		const route = '';
		const content = new Buffer(JSON.stringify(message), 'utf8');
		const properties = {
			// message will survive broker restarts,
			// given it's placed in the durable message queue
			persistent: true,
			// priority: options && options.priority || DEFAULT_PRIORITY,
			contentType: 'application/json',
			contentEncoding: 'utf8',
			// APP-SPECIFIC OPTIONAL FIELDS:
			appId: appId,
			timestamp: Date.now(),
			type: message.type,
			replyTo: undefined,
			correlationId: undefined,
			messageId: `${message.id || message._id || ''}` || undefined
		};

		const pubResult = channel.publish(exchange, route, content, properties, (err, ok) => err ? reject(err) : resolve(ok));
		if (!pubResult) {
			reject(new Error(`channel.publish returned falsey value: ${pubResult}`));
		}
	});
}

/**
 * Decodes the received message content in accordance with the content-type
 * @param  {Buffer} content     Received message content
 * @param  {String} contentType Received message content type (e.g. application/json)
 * @return {Promise}            Promise resolving to decoded payload
 */
function decodePayload(content, contentType) {
	return new Promise(function (resolve, reject) {
		if (!content || !content.length) {
			return resolve(undefined);
		} else if (contentType === 'application/json') {
			return resolve(JSON.parse(content.toString()));
		} else {
			throw new TypeError(`Unexpected content-type: ${contentType}`);
		}
	});
}


module.exports = class RabbitMqBus {

	constructor(options) {
		if (!options) throw new TypeError('options argument required');
		if (!options.connectionString) throw new TypeError('options.connectionString argument required');
		if (options.durable && !options.queue) throw new TypeError('options.queue argument is required, when options.durable is true');

		this._appId = options.appId || undefined;

		// if empty, a random name will be assigned by server
		this._queueName = options.queue || (options.queuePrefix ? options.queuePrefix + uuid.v4().replace(/-/g, '') : undefined);

		// will survive broker restarts
		this._durable = options.durable || false;

		// scoped to connection
		this._exclusive = !options.queue;

		// The count given is the maximum number of messages sent over the channel that can be awaiting acknowledgement;
		// once there are count messages outstanding, the server will not send more messages on this channel until one
		// or more have been acknowledged. A falsey value for count indicates no such limit.
		this._prefetch = options.prefetch || DEFAULT_PREFETCH;

		this._configureChannel = this._configureChannel.bind(this);
		this._createQueue = this._createQueue.bind(this);

		this.connect(options.connectionString);
	}

	connect(connectionString) {
		if (typeof connectionString !== 'string' || !connectionString.length) throw new TypeError('connectionString argument must be a non-empty String');

		debug(`connecting to ${reconnect.mask(connectionString)}...`);

		this._connection = reconnect(() => amqp.connect(connectionString), null, null, debug);
		this._connection.then(() => this.getChannel());
		this._connection.then(cn => {
			debug('connected');
			return cn;
		}, err => {
			debug('connection failure');
			debug(err);
			throw err;
		});
		return this._connection;
	}

	getChannel() {
		const confirm = true;
		const key = confirm ? '_confirmChannel' : '_channel';
		if (this[key])
			return this[key];

		const createMethod = confirm ? 'createConfirmChannel' : 'createChannel';
		return this[key] = this._connection
			.then(conn => conn[createMethod]())
			.then(this._configureChannel)
			.then(this._createQueue);
	}

	_configureChannel(channel) {
		debug('channel created');
		channel.prefetch(this._prefetch);
		return channel;
	}

	_createQueue(channel) {
		return channel.assertQueue(this._queueName, {
			durable: this._durable,
			exclusive: this._exclusive,
			// maxPriority: MAX_PRIORITY
		}).then(response => {
			debug(`queue '${response.queue}' asserted, ${response.messageCount} messages, ${response.consumerCount} consumers`);
			this._queueName = response.queue;
			return channel;
		});
	}

	on(messageType, handler, options) {
		// amq.direct -- delivers messages to queues based on the message routing key
		// amq.fanout -- broadcasts all the messages it receives to all the queues it knows
		// amq.topic
		return this.getChannel()
			.then(ch => Promise.all([
				ch.assertExchange(messageType, 'fanout'),
				ch.bindQueue(this._queueName, messageType /*, pattern */ ),
				ch.consume(this._queueName, this._onMessage.bind(this, ch, handler))
			]))
			.then(results => {
				debug(`'${messageType}' consumer set up as ${results[2].consumerTag}`);
			})
			.catch(debug);
	}

	_onMessage(channel, handler, message) {
		if (!message) return Promise.resolve();
		if (!message.fields) throw new TypeError('message.fields argument required');
		if (!message.properties) throw new TypeError('message.properties argument required');

		debug(`'${descriptor(message)}' received, passing to handler...`);

		return decodePayload(message.content, message.properties.contentType)
			.then(payload => handler(payload))
			.then(result => {
				debug(`'${descriptor(message)}' processed`);
				channel.ack(message);
				debug(`'${descriptor(message)}' acknowledged`);
			}, err => {
				debug(`'${descriptor(message)}' processing failed`);
				debug(err);
				// second argument indicates whether the message will be re-routed to another channel
				channel.reject(message, false);
				debug(`'${descriptor(message)}' rejected`);
			});
	}

	send(command) {
		if (!command) throw new TypeError('command argument required');
		if (!command.type) throw new TypeError('command.type argument required');

		debug(`sending ${command.type}...`);

		return this._doPublish(...arguments);
	}

	publish(event) {
		if (!event) throw new TypeError('event argument required');
		if (!event.type) throw new TypeError('event.type argument required');

		debug(`publishing '${event.type}'...`);

		return this._doPublish(...arguments);
	}

	_doPublish(message) {
		if (!message) throw new TypeError('message argument required');
		if (!message.type) throw new TypeError('message.type argument required');

		return this.getChannel()
			.then(publish(message, this._appId))
			.then(r => {
				debug(`'${message.type}' accepted by message queue`);
			}, err => {
				debug(`'${message.type}' REJECTED by message queue:`);
				debug(err);
				throw err;
			});
	}
};
