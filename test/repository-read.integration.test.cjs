/** @file Dos instancias de Platform comparten páginas y confirmaciones mediante CAS real en MongoDB. */
'use strict';
const test                     = require('node:test');
const assert                   = require('node:assert/strict');
const mongoose                 = require('mongoose');
const { startIsolatedMongo }   = require('./helpers/isolated-mongo.cjs');
const { readReceipts }         = require('../server/modules/realtime/repositories/read-receipts');
const { reader, readIdentity } = require('./helpers/repository-read.cjs');
test(
  'el recibo compartido avanza una vez y sobrevive al cambio de instancia sin almacenar listas',
  { skip: !process.env.TEST_MONGOD_BINARY },
  /** @param {Object} t Prueba. @returns {Promise<void>} MongoDB real aislado; las páginas del propietario son controladas. */ async (
    t,
  ) => {
    const database = await startIsolatedMongo({
      binary : process.env.TEST_MONGOD_BINARY,
      mongoose,
    });
    t.after(database.stop);
    const collection = database.connection.db.collection('realtime_read_receipts');
    await collection.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
    const receipts = readReceipts(database.connection);
    await receipts.verify();
    /** @param {Object} query Página. @returns {Promise<Object>} Cursor de revisión consistente del productor. */
    async function manifest(query) {
      return {
        statusCode : 200,
        data       : {
          repository  : { id: 'aaaaaaaaaaaaaaaaaaaaaaaa' },
          revision    : 5,
          items       : [
            {
              id       : query.cursor ? 'second' : 'first',
              revision : 1,
            },
          ],
          nextCursor  : query.cursor ? null : 'next',
          notModified : false,
        },
      };
    }
    const one = reader({
        receipts,
        manifest,
      }),
      two     = reader({
        receipts : readReceipts(database.connection),
        manifest,
      });
    const identity   = readIdentity({
      userCode        : 'U1',
      activeWorkspace : 'W1',
      delegation      : {
        grantCode : 'G1',
        clientId  : 'C1',
      },
    });
    const request    = {
      subscriptionId : 'repo',
      watchVersion   : 1,
      repositoryId   : 'aaaaaaaaaaaaaaaaaaaaaaaa',
    };
    const registered = await one.register(request, identity);
    const input      = {
      subscriptionId : request.subscriptionId,
      watchVersion   : 1,
      receiptId      : registered.receiptId,
      query          : { repositoryId: request.repositoryId },
    };
    const result     = await Promise.allSettled([one.read(input, identity), two.read(input, identity)]);
    assert.equal(
      result.filter(
        /** @param {Object} item Resultado. @returns {boolean} Un solo ganador del CAS. */ (item) =>
          item.status === 'fulfilled',
      ).length,
      1,
    );
    await assert.rejects(two.bind(input, identity), { statusCode: 409 });
    const page = await two.read(
      {
        ...input,
        pageCursor : 'next',
      },
      identity,
    );
    assert.equal((await one.bind(input, identity)).cursor, page.control.cursor);
    const stored = await collection.findOne({ _id: input.receiptId });
    assert.equal(stored.count, 2);
    assert.equal(stored.state, 'ready');
    assert.equal(stored.data, undefined);
    assert.equal(stored.items, undefined);
    assert.equal(stored.revision, 5);
    await collection.updateOne({ _id: stored._id }, { $set: { expiresAt: new Date(0) } });
    await assert.rejects(one.bind(input, identity), { statusCode: 409 });
  },
);
