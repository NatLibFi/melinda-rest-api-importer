import {handleInterrupt} from '@natlibfi/melinda-backend-commons';
import {logError} from '@natlibfi/melinda-rest-api-commons';
import startApp from './app.js';
import * as config from './config.js';

run();

async function run() {
  registerInterruptionHandlers();

  await startApp(config);

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

    function handleTermination({code = 0, message}) {
      logMessage(message);
      process.exit(code); // eslint-disable-line no-process-exit
    }

    function handleSignal(signal) {
      handleTermination({code: 1, message: `Received ${signal}`});
    }

    function logMessage(message) {
      if (message) {
        logError(`Are we here?`);
        logError('No catch error!');
        return logError(message);
      }
    }
  }
}
