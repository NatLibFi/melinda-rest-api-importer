import express from 'express';
import path from 'path';
import HttpStatus from 'http-status';
import {Utils} from '@natlibfi/melinda-commons';

import {logError} from './utils';

import {createWwwRoute, createToQueueRequestRouter} from './routes';

import {
    IP_FILTER_BIB
} from './config';

const app = express();

const {createLogger, createExpressLogger} = Utils;
const ipFilterList = JSON.parse(IP_FILTER_BIB).map(rule => new RegExp(rule));

const logger = createLogger();

logger.log('debug', 'loading middlewares');
app.use(createExpressLogger());
app.use(createWhitelistMiddleware(ipFilterList));
app.use(express.urlencoded({extended: false}));
app.use(express.static(path.join(__dirname, '../public')));

logger.log('debug', 'loading routes');
app.use('/', createWwwRoute);
app.use('/toQueue', createToQueueRequestRouter());
app.use(handleError);

export default app;

function createWhitelistMiddleware(whitelist) {
    return (req, res, next) => {
        const ip = req.ip.split(/:/).pop();

        if (whitelist.some(pattern => pattern.test(ip))) {
            return next();
        }

        res.sendStatus(HttpStatus.FORBIDDEN);
    };
}

function handleError(err, req, res, next) { // eslint-disable-line no-unused-vars
	if (err instanceof Error || 'status' in err) {
		res.sendStatus(err.status);
	} else {
		logError(err)
		res.sendStatus(HttpStatus.INTERNAL_SERVER_ERROR);
	}
  }