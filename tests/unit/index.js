'use strict';

const {expect} = require('chai');
const RabbitMqBus = require('../../');

describe('RabbitMqBus', function () {

	it('is default export', () => {
		expect(RabbitMqBus).to.be.a('Function');
	});

	it('validates constructor arguments', () => {
		expect(() => new RabbitMqBus()).to.throw(TypeError);
	});
});
