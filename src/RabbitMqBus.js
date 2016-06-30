'use strict';

const amqp = require('amqplib');
const uuid = require('uuid');
const debug = require('debug');
const reconnect = require('./reconnect');

const DEFAULT_PREFETCH = 100;
const _channel = Symbol('channel');
const _queueName = Symbol('queue');
const _handlers = Symbol('consumers');
const _consumerTag = Symbol('consumerTag');


/**
 * Creates human-readable message descriptor for debug messages
 *
 * @param {{fields:object, properties:object}} message
 * @returns {string}
 */
function descriptor(message) {
	if (!message) throw new TypeError('message argument required');

	return [
		message.fields && message.fields.exchange,
		message.properties && message.properties.messageId || message.fields && message.fields.deliveryTag,
		message.fields && message.fields.redelivered ? 'redelivered' : undefined
	].filter(v => !!v).join('-');
}


/**
 * Publishes a message to the given MQ channel/exchange
 *
 * @param {object} channel
 * @param {{type:string}} message
 * @param {string} appId
 * @param {function(string):void} debug
 */
function publish(channel, message, appId, debug) {
	if (!channel) throw new TypeError('channel argument required');
	if (!message) throw new TypeError('message argument required');
	if (!message.type) throw new TypeError('message.type argument required');
	if (appId && typeof appId !== 'string') throw new TypeError('appId argument, when provided, must be a String');
	if (typeof debug !== 'function') throw new TypeError('debug argument must be a Function');

	return new Promise((rs, rj) => {

		const content = new Buffer(JSON.stringify(message), 'utf8');
		const properties = {
			// message will survive broker restarts, given it's placed in the durable message queue
			persistent: true,
			// priority: options && options.priority || DEFAULT_PRIORITY,
			contentType: 'application/json',
			contentEncoding: 'utf8',
			// APP-SPECIFIC OPTIONAL FIELDS:
			appId: appId,
			timestamp: Date.now(),
			type: message.type,
			// replyTo: undefined,
			// correlationId: undefined,
			messageId: `${message.id || message._id || ''}` || undefined
		};

		const r = channel.publish(message.type, '', content, properties, (err, ok) => err ? rj(err) : rs(ok));
		if (!r) {
			rj(new Error(`channel.publish returned falsey value: ${r}`));
		}
	}).then(r => {
		debug(`'${message.type}' acknowledged by queue`);
	}, err => {
		debug(`'${message.type}' REJECTED by queue:`);
		debug(err);
		throw err;
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


function setupQueueExchange(channel, queueName, exchangeName) {
	if (!channel) throw new TypeError('channel argument required');
	if (typeof queueName !== 'string' || !queueName.length) throw new TypeError('queueName argument must be a non-empty String');
	if (typeof exchangeName !== 'string' || !exchangeName.length) throw new TypeError('exchangeName argument must be a non-empty String');

	// amq.direct -- delivers messages to queues based on the message routing key
	// amq.fanout -- broadcasts all the messages it receives to all the queues it knows
	// amq.topic

	return Promise.resolve()
		.then(() => channel.assertExchange(exchangeName, 'fanout'))
		.then(() => channel.bindQueue(queueName, exchangeName /*, pattern */));
}

function setupDeadLetterQueue(channel, queueName) {
	if (!channel) throw new TypeError('channel argument required');
	if (typeof queueName !== 'string' || !queueName.length) throw new TypeError('name argument must be a non-empty String');

	return Promise.resolve()
		.then(() => channel.assertQueue(queueName, { durable: true }))
		.then(() => setupQueueExchange(channel, queueName, queueName));
}

/**
 * Setup main queue, dead-letter queue and consumer
 *
 * @param {object} channel
 * @param {{queue: string, queuePrefix: string, durable: boolean}} options
 * @param {function(object):PromiseLike<void>} handler
 * @param {function(string):void} debug
 * @returns {{queue:string, consumerTag: string, channel: object}}
 */
function setupQueue(channel, options, handler, debug) {
	if (!channel) throw new TypeError('channel argument required');
	if (!options) throw new TypeError('options argument required');
	if (typeof handler !== 'function') throw new TypeError('handler argument must be a Function');
	if (typeof debug !== 'function') throw new TypeError('debug argument must be a Function');

	// if empty, a random name will be assigned by server
	const queueName = options.queue || (options.queuePrefix ? options.queuePrefix + uuid.v4().replace(/-/g, '') : undefined);
	// will survive broker restarts
	const durable = options.durable || false;
	// scoped to connection
	const exclusive = !options.queue;
	// an exchange to which messages discarded from the queue will be resent
	const deadLetterExchange = options.durable ? queueName + '.failed' : undefined;

	const result = { channel };

	return Promise.resolve()
		.then(() => deadLetterExchange ? setupDeadLetterQueue(channel, deadLetterExchange) : undefined)
		.then(() => channel.assertQueue(queueName, { durable, exclusive, deadLetterExchange }))
		.then(response => {
			debug(`queue '${response.queue}' asserted, ${response.messageCount} messages, ${response.consumerCount} consumers`);
			result.queue = response.queue;
		})
		.then(() => channel.consume(result.queue, handler))
		.then(response => {
			debug(`consumer set up as ${response.consumerTag}`);
			result.consumerTag = response.consumerTag;
		})
		.then(() => result);
}


module.exports = class RabbitMqBus {

	get channel() {
		return this[_channel];
	}

	/**
	 * Creates an instance of RabbitMqBus
	 *
	 * @param {{connectionString:string, appId:string, queue:string, queuePrefix:string, durable: boolean, prefetch:number}} options
	 */
	constructor(options) {
		if (!options) throw new TypeError('options argument required');
		if (typeof options.connectionString !== 'string' || !options.connectionString.length)
			throw new TypeError('options.connectionString argument must be a non-empty String');
		if (options.durable && !options.queue)
			throw new TypeError('options.queue argument is required, when options.durable is true');

		this._appId = options.appId || undefined;
		this._debug = debug('cqrs:RabbitMqBus' + (this._appId ? ':' + this._appId : ''));

		this[_handlers] = {};
		this._handle = this._handle.bind(this);

		this._debug(`connecting to ${reconnect.mask(options.connectionString)}...`);

		this[_channel] = reconnect(() => amqp.connect(options.connectionString), null, null, this._debug)
			.then(connection => connection.createConfirmChannel())
			.then(channel => {
				this._debug('connected, channel created');
				channel.prefetch('prefetch' in options ? options.prefetch : DEFAULT_PREFETCH);
				return channel;
			})
			.then(channel => setupQueue(channel, options, this._handle, this._debug))
			.then(result => {
				this[_queueName] = result.queue;
				this[_consumerTag] = result.consumerTag;
				return result.channel;
			})
			.catch(err => this._debug(err));
	}

	on(messageType, handler) {
		if (typeof messageType !== 'string' || !messageType.length) throw new TypeError('messageType argument must be a non-empty String');
		if (typeof handler !== 'function') throw new TypeError('handler argument must be a Function');

		(this[_handlers][messageType] || (this[_handlers][messageType] = [])).push(handler);

		return this.channel.then(ch => setupQueueExchange(ch, this[_queueName], messageType)).catch(err => {
			this._debug(err);
			throw err;
		});
	}

	_handle(message) {
		if (!message) {
			this._debug('empty message received, ignoring');
			return;
		}

		const msgId = descriptor(message);

		this._debug(`'${msgId}' received`);

		return decodePayload(message.content, message.properties.contentType)
			.then(payload => Promise.all(this[_handlers][message.properties.type].map(handler => handler(payload))))
			.then(results => {
				this._debug(`'${msgId}' processed by ${results.length === 1 ? '1 handler' : results.length + ' handlers'}, acknowledging...`);

				return this.channel.then(channel => channel.ack(message)).then(() => {
					this._debug(`'${msgId}' acknowledged`);
				});
			}, err => {
				this._debug(`'${msgId}' processing failed: %j`, err);

				// second argument indicates whether the message will be re-routed to another channel
				return this.channel.then(channel => channel.reject(message, false)).then(() => {
					this._debug(`'${msgId}' rejected`);
				});
			});
	}


	/**
	 * Removes subscriptions
	 *
	 * @param {string} [messageType] Optional message type which subscriptions must be destroyed. If not provided, all subscriptions will be destroyed
	 * @returns {void}
	 */
	off(messageType) {
		if (messageType) {
			this._debug(`unsubscribing from '${messageType}'`);
			delete this[_handlers][messageType];
		}
		else {
			this._debug('unsubscribing from all messages');
			this[_handlers] = {};
		}
	}

	/**
	 * Sends a command to MQ
	 *
	 * @param {{type:string}} command
	 * @returns {PromiseLike<void>}
	 */
	send(command) {
		if (!command) throw new TypeError('command argument required');
		if (!command.type) throw new TypeError('command.type argument required');

		this._debug(`sending ${command.type}...`);

		return this.channel.then(ch => publish(ch, command, this._appId, this._debug));
	}

	/**
	 * Publishes an event to MQ
	 *
	 * @param {{type:string}} event
	 * @returns {PromiseLike<void>}
	 */
	publish(event) {
		if (!event) throw new TypeError('event argument required');
		if (!event.type) throw new TypeError('event.type argument required');

		this._debug(`publishing '${event.type}'...`);

		return this.channel.then(ch => publish(ch, event, this._appId, this._debug));
	}
};
