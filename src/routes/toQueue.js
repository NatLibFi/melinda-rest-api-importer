import HttpStatus from 'http-status';
import {pushToQueue} from '../services/toQueueService';
import express, {Router} from 'express';
import {Utils} from '@natlibfi/melinda-commons';
import uuid from 'uuid';
import {EMITTER} from '../bin/www';
import fileUpload from 'express-fileupload';
import fs from 'fs';
import readline from 'readline';
import path from 'path';
import {once} from 'events';

import {
	IP_FILTER_BIB, FORMATS, TMP_FILE_LOCATION,
	NAME_QUEUE_BULK, NAME_QUEUE_PRIORITY,
	EMITTER_CONSUM_ANNOUNSE, CHUNK_SIZE
} from '../config';
import {logError, validateLine} from '../utils';

const {createLogger} = Utils;

export default function () {
	const CONTENT_TYPES = {
		'application/json': FORMATS.JSON,
		'application/marc': FORMATS.ISO2709,
		'application/xml': FORMATS.MARCXML,
		'text/plain': FORMATS.TEXT
	};
	const ipFilterList = JSON.parse(IP_FILTER_BIB).map(rule => new RegExp(rule));
	const logger = createLogger();

	return new Router()
		.use(createWhitelistMiddleware(ipFilterList))
		.post('/', express.json(), priorityBlob)
		.post('/bulk/:operation', fileUpload({
			limits: {fileSize: 50 * 1024 * 1024},
			debug: true
		}), bulkBlob);

	async function priorityBlob(req, res, next) {
		logger.log('debug', 'Priority blob');
		const QUEUEID = uuid.v1();
		try {
			let back = [];
			let counter = 0;
			// check content type
			const type = req.headers['content-type'];
			const format = CONTENT_TYPES[type];
			// 1: json, 2: marc, 3: xml, 4: text
			if (!format) {
				return res.sendStatus(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
			}
			const unique = req.query.unique === undefined ? true : formatRequestBoolean(req.query.unique);
			const noop = formatRequestBoolean(req.query.noop);

			logger.log('debug', 'Sending now!')
			counter = req.body.records.length;
			await new Promise((resolve, reject) => {
				// send to queue
				//console.log(req.body);
				pushToQueue(NAME_QUEUE_PRIORITY, QUEUEID, format, req.body.records);
				EMITTER.on(EMITTER_CONSUM_ANNOUNSE + '.' + QUEUEID, async (record) => {
					back.push(record);
					counter--;
					logger.log('debug', `Emitter got back ${back}`);
					if (counter === 0) {
						resolve();
					}
				});
			});

			// send resonse
			res
				.json({
					format, unique, noop, back,
					request_body: req.body
				});
		} catch (err) {
			logger.log('debug', `Error in /priority POST ${err}`);
			res.sendStatus(HttpStatus[400]);
		}
	}

	async function bulkBlob(req, res, next) {
		logger.log('debug', 'Bulk blob');
		let operation = req.params.operation;
		const QUEUEID = uuid.v1();

		if (operation !== 'update' && operation !== 'create'  && operation !== undefined) {
			return res.sendStatus(HttpStatus[400]);
		}

		if (!req.files) {
			return res.sendStatus(HttpStatus[422]);
		}

		try {
			logger.log('debug', 'Sending now!')
			let amount = 0;
			let index = 1;
			const back = [];
			const records = [];
			let record = [];
			let currentRecordId = '';
			logger.log('debug', req.files.file.name);
			logger.log('debug', req.files.file.mimetype);
			await req.files.file.mv(TMP_FILE_LOCATION + QUEUEID);

			const stream = fs.createReadStream(path.resolve('./tmp/' + QUEUEID));
			const rl = readline.createInterface({
				input: stream,
				crlfDelay: Infinity
			});

			rl.on('line', async line => {
				const validation = await validateLine(line, index, operation);
				//logger.log('debug', JSON.stringify(validation));
				if (validation.valid){
					{currentRecordId === '' ? currentRecordId = validation.id : null};
					if (currentRecordId !== validation.id && record.length > 0) {
						records.push(record);
						record = [];
						if (records.length > CHUNK_SIZE) {
							const chunk = lines.splice(0, CHUNK_SIZE);
							amount += chunk.length;
							pushToQueue(NAME_QUEUE_BULK, QUEUEID, 'alephseq', chunk, operation);
						}
						currentRecordId = validation.id;
					}
					record.push(line);
				}
			}).on('close', async () => {
				if (record.length > 0) {
					records.push(record);
					amount += records.length;
					await pushToQueue(NAME_QUEUE_BULK, QUEUEID, 'alephseq', records, operation);
				}
			});

			await once(rl, 'close');
			back.push({QUEUEID, 'numRecords': amount, 'queueType': NAME_QUEUE_BULK})
			logger.log('debug', `File spliced and passed ${amount} total records`);
			res.send(back);
		} catch (err) {
			logError(err);
			res.sendStatus(HttpStatus[400]);
		}
	}

	function createWhitelistMiddleware(whitelist) {
		return (req, res, next) => {
			const ip = req.ip.split(/:/).pop();

			if (whitelist.some(pattern => pattern.test(ip))) {
				return next();
			}

			res.sendStatus(HttpStatus.FORBIDDEN);
		};
	}

	function formatRequestBoolean(value) {
		if (Number.isNaN(Number(value))) {
			return value === 'true';
		}

		return Boolean(Number(value));
	}
}