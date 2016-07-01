'use strict';

const amqp = require('amqplib');
const uuid = require('uuid');
const debug = require('debug');
const reconnect = require('./reconnect');

const _channel = Symbol('channel');
const _queueName = Symbol('queue');
const _handlers = Symbol('consumers');
const _options = Symbol('options');


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


/**
 * Creates queue
 *
 * @template TChannel
 * @param {TChannel} channel
 * @param {string} queueName
 * @param {{durable:boolean, exclusive:boolean, deadLetterExchange:string}} options
 * @param {function(string):void} debug
 * @returns {PromiseLike<{channel:TChannel, queueName:string}>}
 */
function assertQueue(channel, queueName, options, debug) {
	if (!channel) throw new TypeError('channel argument required');
	if (typeof queueName !== 'string' || !queueName.length) throw new TypeError('queueName argument must be a non-empty String');
	if (!options) throw new TypeError('options argument required');
	if (typeof debug !== 'function') throw new TypeError('debug argument must be a Function');

	debug(`asserting queue '${queueName}'...`);

	return Promise.all([
		channel.assertQueue(queueName, options),
		options.deadLetterExchange ? assertDeadLetterExchange(channel, options.deadLetterExchange, debug) : undefined
	]).then(r => {
		if (r[0]) {
			debug(`queue '${r[0].queue}' asserted, ${r[0].messageCount} messages, ${r[0].consumerCount} consumers`);
			queueName = r[0].queue;
		}
		return { channel, queueName };
	});
}

/**
 * Asserts exchange and binds a ginen queue to it
 *
 * @template TChannel
 * @param {TChannel} channel
 * @param {string} queueName
 * @param {string} exchangeName
 * @param {function(string):void} debug
 * @returns {PromiseLike<TChannel>}
 */
function assertExchange(channel, queueName, exchangeName, debug) {
	if (!channel) throw new TypeError('channel argument required');
	if (typeof queueName !== 'string' || !queueName.length) throw new TypeError('queueName argument must be a non-empty String');
	if (typeof exchangeName !== 'string' || !exchangeName.length) throw new TypeError('exchangeName argument must be a non-empty String');
	if (typeof debug !== 'function') throw new TypeError('debug argument must be a Function');

	debug(`asserting exchange '${exchangeName}' monitored by '${queueName}'...`);

	return Promise.all([
		channel.assertExchange(exchangeName, 'fanout'),
		channel.bindQueue(queueName, exchangeName /*, pattern */),
	]).then(() => channel);
}

/**
 * Asserts dead letter exchange with a durable queue for failed messages
 *
 * @param {TChannel} channel
 * @param {string} deadLetterExchange
 * @param {function(string):void} debug
 * @returns {PromiseLike<TChannel>}
 */
function assertDeadLetterExchange(channel, deadLetterExchange, debug) {
	if (!channel) throw new TypeError('channel argument required');
	if (typeof deadLetterExchange !== 'string' || !deadLetterExchange.length)
		throw new TypeError('deadLetterExchange argument must be a non-empty String');
	if (typeof debug !== 'function') throw new TypeError('debug argument must be a Function');

	return Promise.resolve(channel)
		.then(channel => assertQueue(channel, deadLetterExchange, { durable: true }, debug))
		.then(({channel}) => assertExchange(channel, deadLetterExchange, deadLetterExchange, debug));
}

/**
 * Subscribes the channel to the given queue
 *
 * @param {string} queueName
 * @param {any} handler
 * @param {function(string):void} debug
 * @returns {function(object):PromiseLike<object>}
 */
function assertConsumer(channel, queueName, handler, debug) {

	debug(`subscribing to queue '${queueName}'...`);

	return channel.consume(queueName, handler).then(response => {
		debug(`consumer set up as ${response.consumerTag}`);
		return channel;
	});
}


module.exports = class RabbitMqBus {

	get channel() {
		return this[_channel];
	}

	get queueName() {
		return this[_queueName];
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
		this._handle = this._handle.bind(this);

		this[_queueName] = options.queue || options.queuePrefix && (options.queuePrefix + uuid.v4().replace(/-/g, '')) || undefined;
		this[_options] = {
			// will survive broker restarts
			durable: options.durable || false,
			// scoped to connection
			exclusive: !options.queue,
			// an exchange to which messages discarded from the queue will be resent
			deadLetterExchange: options.durable ? this.queueName + '.failed' : undefined
		};

		this._debug(`connecting to ${reconnect.mask(options.connectionString)}...`);

		this[_channel] = reconnect(() => amqp.connect(options.connectionString), null, null, this._debug)
			.then(connection => connection.createConfirmChannel())
			.then(channel => {
				this._debug('connected, channel created');
				if ('prefetch' in options)
					channel.prefetch(options.prefetch);
				return channel;
			});
	}

	on(messageType, handler) {
		if (typeof messageType !== 'string' || !messageType.length) throw new TypeError('messageType argument must be a non-empty String');
		if (typeof handler !== 'function') throw new TypeError('handler argument must be a Function');


		let subscribeSequence = this.channel;

		if (!this[_handlers]) {
			this[_handlers] = {};

			subscribeSequence = subscribeSequence
				.then(channel => assertQueue(channel, this.queueName, this[_options], this._debug))
				.then(({channel, queueName}) => {
					this[_queueName] = queueName;
					return channel;
				})
				.then(channel => assertConsumer(channel, this.queueName, this._handle, this._debug));
		}

		if (!(messageType in this[_handlers])) {
			this[_handlers][messageType] = [handler];

			subscribeSequence = subscribeSequence
				.then(channel => assertExchange(channel, this.queueName, messageType, this._debug));
		}
		else {
			this[_handlers][messageType].push(handler);
		}

		return subscribeSequence.catch(err => {
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
				this._debug(`'${msgId}' processing failed: %s`, err && err.message);
				this._debug(err);

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
