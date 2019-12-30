import {Utils} from '@natlibfi/melinda-commons/';

const {readEnvironmentVariable, parseBoolean} = Utils; // eslint-disable-line no-unused-vars

export const CHUNK_SIZE = readEnvironmentVariable('CHUNK_SIZE', {defaultValue: 50, format: v => Number(v)});

export const AMQP_URL = JSON.parse(readEnvironmentVariable('AMQP_URL'));

// Same as in import -> Move to melinda-record-import-commons constants?
export const NAME_QUEUE_PRIORITY = 'PRIORITY';
export const NAME_QUEUE_BULK = 'BULK';
export const NAME_QUEUE_REPLY_BULK = 'REPLY_BULK';
export const NAME_QUEUE_REPLY_PRIO = 'REPLY_PRIO';
export const PURGE_QUEUE_ON_LOAD = readEnvironmentVariable('PURGE_QUEUE_ON_LOAD', {defaultValue: 0}); // ParseBoolean works?

// About same as in Rest-Api -> Move to melinda-record-import-commons constants?
export const BLOB_STATE = {
	CREATED: 'CREATED',
	UPDATED: 'UPDATED',
	ACTION_NEEDED: 'ACTION_NEEDED',
	ERROR: 'ERROR'
};

export const SRU_URL = readEnvironmentVariable('SRU_URL', '[".*"]');
export const RECORD_LOAD_URL = readEnvironmentVariable('RECORD_LOAD_URL', '');
export const RECORD_LOAD_API_KEY = readEnvironmentVariable('RECORD_LOAD_API_KEY', '');
export const RECORD_LOAD_LIBRARY = readEnvironmentVariable('RECORD_LOAD_LIBRARY', '');
