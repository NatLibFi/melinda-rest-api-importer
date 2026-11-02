// Ambient module declaration for @natlibfi/marc-record-serializers: the package ships no
// types and there is no @types package on npm, and TypeScript does not infer types for
// node_modules JavaScript, so without this the import in loadStarter.ts is an implicit
// any (TS7016 under noImplicitAny).
// Minimal on purpose: only the member this repo imports and calls — AlephSequential.to —
// derived from the package source (src/aleph-sequential.js): the record parameter is a
// MarcRecord (it calls record.get / record.leader / record.removeFields), the second
// parameter is the boolean useCrForContinuingResource flag, and the function returns
// the Aleph-sequential string it builds.
declare module '@natlibfi/marc-record-serializers' {
  import type {MarcRecord} from '@natlibfi/marc-record';

  export const AlephSequential: {
    to(record: MarcRecord, useCrForContinuingResource?: boolean): string;
  };
}
