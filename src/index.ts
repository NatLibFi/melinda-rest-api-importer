import {handleInterrupt} from '@natlibfi/melinda-commons';
import {logError} from '@natlibfi/melinda-rest-api-commons';
import startApp, {type Operation} from './app.ts';
import * as config from './config.ts';

run();

async function run() {
  registerInterruptionHandlers();

  await startApp({...config, operation: config.operation as Operation});

  function registerInterruptionHandlers() {
    process
      .on('SIGTERM', handleSignal)
      .on('SIGINT', handleInterrupt)
      .on('uncaughtException', ({stack}) => {
        handleTermination({code: 1, message: stack});
      })
      .on('unhandledRejection', ({stack}) => {
        handleTermination({code: 1, message: stack});
      });

    function handleTermination({code = 0, message}: {code?: number; message?: unknown}) {
      logMessage(message);
      process.exit(code);
    }

    function handleSignal(signal: NodeJS.Signals) {
      handleTermination({code: 1, message: `Received ${signal}`});
    }

    function logMessage(message: unknown) {
      if (message) {
        logError(`Are we here?`);
        logError('No catch error!');
        return logError(message);
      }
    }
  }
}
