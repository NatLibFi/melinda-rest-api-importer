import {Utils} from '@natlibfi/melinda-commons';
import {validateLine} from '../utils';
import {Datastore} from './datastoreService'

import {RECORD_STATE} from '../config';

const {createLogger} = Utils;
//const {createService} = Datastore;

export function toRecordLoadApi() {
    const logger = createLogger();
    //const DatastoreService = createService();

    return async data => {
        let record;
        let id = 0;

        logger.log('info', `Has format ${data.format}`);

        if (data.format === 'application/json') {
            record = new MarcRecord(JSON.parse(data.record));
        } else if (data.format === 'alephseq') {
            record = data.record.join('\n');
        }

        logger.log('info', `Has record ${record}`);
        logger.log('info', `Has operation ${data.operation}`);

        // TODO if record type Alephsequental = needs changes in commons
        // TODO Catalogger indetification
        if (record) {
            if (data.operation === 'update') {
                // async function update({record, id, cataloger = DEFAULT_CATALOGER_ID, indexingPriority = INDEXING_PRIORITY.HIGH}) {
                if (data.format === 'application/json') {
                    id = record.id;
                    //await DatastoreService.updateJSON({record, id, cataloger: 'IMP_HELMET'});
                } else if (data.format === 'alephseq') {
                    const validation = await validateLine(record);
                    id = validation.id;
                    //await DatastoreService.updateALEPH({record, id, cataloger: 'IMP_HELMET'});
                }
                logger.log('info', `Updated record ${id}`);
                return {status: RECORD_STATE.UPDATED, metadata: {id}};
            } else if (operation === 'create') {
                if (data.format === 'application/json') {
                    //id = await DatastoreService.createJSON({record, cataloger: 'IMP_HELMET'});
                } else if (data.format === 'alephseq') {
                    //id = await DatastoreService.createALEPH({record, cataloger: 'IMP_HELMET'});
                }
                logger.log('info', `Created new record ${id}`);
                return {status: RECORD_STATE.CREATED, metadata: {id}};
            }
        }
        return false;
    };
}