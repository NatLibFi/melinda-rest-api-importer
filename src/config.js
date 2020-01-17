import {Utils} from '@natlibfi/melinda-commons/';

const {readEnvironmentVariable, parseBoolean} = Utils; // eslint-disable-line no-unused-vars

// Useless?
export const SRU_URL = readEnvironmentVariable('SRU_URL');

// Record-load-api to save data
export const RECORD_LOAD_URL = readEnvironmentVariable('RECORD_LOAD_URL');
export const RECORD_LOAD_API_KEY = readEnvironmentVariable('RECORD_LOAD_API_KEY');
export const RECORD_LOAD_LIBRARY = readEnvironmentVariable('RECORD_LOAD_LIBRARY');

// Mongo to reply bulk
export const MONGO_URI = readEnvironmentVariable('MONGO_URI', {defaultValue: 'mongodb://localhost:27017/db'});
export const MONGO_POOLSIZE = readEnvironmentVariable('MONGO_POOLSIZE', {defaultValue: 200, format: v => Number(v)});
export const MONGO_DEBUG = readEnvironmentVariable('MONGO_DEBUG', {defaultValue: true});

// Rabbit queue variables
export const AMQP_URL = JSON.parse(readEnvironmentVariable('AMQP_URL'));
// Example: '{"name": "QUEUE_NAME", "style": "CONSUME_STYLE"}'
export const QUEUE = readEnvironmentVariable('QUEUE');
export const OPERATION = readEnvironmentVariable('OPERATION');

export const [OFFLINE_BEGIN, OFFLINE_DURATION] = readEnvironmentVariable('OFFLINE_PERIOD', {defaultValue: '0,0'}).split(',');
export const PURGE_QUEUE_ON_LOAD = Boolean(readEnvironmentVariable('PURGE_QUEUE_ON_LOAD', {defaultValue: 1})); // ParseBoolean works?
