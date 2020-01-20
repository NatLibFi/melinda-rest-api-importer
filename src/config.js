import {Utils} from '@natlibfi/melinda-commons/';

const {readEnvironmentVariable} = Utils; // eslint-disable-line no-unused-vars

// Useless?
export const SRU_URL = readEnvironmentVariable('SRU_URL');

// Record-load-api to save data
export const RECORD_LOAD_URL = readEnvironmentVariable('RECORD_LOAD_URL');
export const RECORD_LOAD_API_KEY = readEnvironmentVariable('RECORD_LOAD_API_KEY');
export const RECORD_LOAD_LIBRARY = readEnvironmentVariable('RECORD_LOAD_LIBRARY');
export const DEFAULT_CATALOGER_ID = readEnvironmentVariable('DEFAULT_CATALOGER_ID', {defaultValue: 'API'});

// Mongo variables
export const MONGO_URI = readEnvironmentVariable('MONGO_URI', {defaultValue: 'mongodb://localhost:27017/db'});

// Rabbit queue variables
export const AMQP_URL = JSON.parse(readEnvironmentVariable('AMQP_URL'));
export const PURGE_QUEUE_ON_LOAD = Boolean(readEnvironmentVariable('PURGE_QUEUE_ON_LOAD', {defaultValue: 1}));
export const POLL_WAIT_TIME = readEnvironmentVariable('POLL_WAIT_TIME', {defaultValue: 1000});

// Operation variables
export const QUEUE = readEnvironmentVariable('QUEUE');
export const OPERATION = readEnvironmentVariable('OPERATION');

export const [OFFLINE_BEGIN, OFFLINE_DURATION] = readEnvironmentVariable('OFFLINE_PERIOD', {defaultValue: '0,0'}).split(',');
