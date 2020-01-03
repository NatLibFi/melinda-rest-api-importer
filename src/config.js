import {Utils} from '@natlibfi/melinda-commons/';

const {readEnvironmentVariable, parseBoolean} = Utils; // eslint-disable-line no-unused-vars

export const AMQP_URL = JSON.parse(readEnvironmentVariable('AMQP_URL'));
export const SRU_URL = readEnvironmentVariable('SRU_URL');
export const RECORD_LOAD_URL = readEnvironmentVariable('RECORD_LOAD_URL');
export const RECORD_LOAD_API_KEY = readEnvironmentVariable('RECORD_LOAD_API_KEY');
export const RECORD_LOAD_LIBRARY = readEnvironmentVariable('RECORD_LOAD_LIBRARY');

export const [OFFLINE_BEGIN, OFFLINE_DURATION] = readEnvironmentVariable('OFFLINE_PERIOD', {defaultValue: '0,0'}).split(',');
export const PURGE_QUEUE_ON_LOAD = readEnvironmentVariable('PURGE_QUEUE_ON_LOAD', {defaultValue: 0}); // ParseBoolean works?
