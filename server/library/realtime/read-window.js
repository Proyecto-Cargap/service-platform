/** @file Coordina una lectura HTTP acreditada y cambios realtime de una única sección. */
'use strict';
const { randomUUID }                 = require('node:crypto');
const { contentCursor, windowDelta } = require('./window-delta');
const { validatedSnapshot }          = require('./snapshot');

/** Serializa cambios por suscripción sin entregar listas al registrarla. */
class ReadWindow {
  /** @param {Object} options Intención, lector canónico, autorización y entrega ya compuestos. */
  constructor({
    subscriptionId,
    watchVersion,
    topic,
    section,
    read,
    authorize,
    emit,
    onClose,
    collections = {},
    maxOperations = 1024,
    maxBytes = 1024 * 1024,
  }) {
    this.subscriptionId = subscriptionId;
    this.watchVersion = watchVersion;
    this.topic = topic;
    this.section = section;
    this.read = read;
    this.authorize = authorize;
    this.emit = emit;
    this.onClose = onClose;
    this.collections = collections;
    this.maxOperations = maxOperations;
    this.maxBytes = maxBytes;
    this.delivery = 0;
    this.closed = false;
    this.bound = false;
    this.dirty = false;
    this.changes = [];
  }

  /** @returns {Object} Confirma exclusivamente el control de la suscripción. */
  ready() {
    return {
      statusCode     : 200,
      subscriptionId : this.subscriptionId,
      watchVersion   : this.watchVersion,
    };
  }

  /**
   * Instala exclusivamente el recibo que el backend obtuvo de una respuesta HTTP autorizada.
   * @param {Object} receipt Datos acreditados por el almacén de recibos, nunca por el cuerpo del socket.
   * @returns {Promise<void>} Reproduce los cambios ocurridos durante la lectura.
   */
  async bind(receipt) {
    if (
      this.closed ||
      this.bound ||
      this.binding ||
      receipt.watchVersion !== this.watchVersion ||
      receipt.subscriptionId !== this.subscriptionId
    )
      throw Object.assign(new Error('SUPERSEDED'), { statusCode: 409 });
    this.binding = true;
    try {
      await this.authorize();
      if (this.closed) throw Object.assign(new Error('SUPERSEDED'), { statusCode: 409 });
      const { cursor } = validatedSnapshot(receipt);
      if (contentCursor(receipt.data) !== receipt.projectionHash)
        throw new Error('INVALID_READ_CURSOR');
      this.base = receipt.data;
      this.cursor = cursor;
      this.query = receipt.query;
      this.bound = true;
    } finally {
      this.binding = false;
    }
    if (this.dirty) await this.drain();
  }

  /** @param {Object} [change] Referencia durable acotada del productor. @returns {Promise<void>} Acumula cambios durante HTTP y agrupa los recibidos durante una lectura. */
  change(change) {
    if (this.closed) return Promise.resolve();
    if (change) {
      if (this.changes.length >= 128) {
        this.send('resync-required', { reason: 'QUEUE_OVERFLOW' });
        this.close();
        return Promise.resolve();
      }
      this.changes.push(change);
    }
    this.dirty = true;
    return this.bound ? this.drain() : Promise.resolve();
  }

  /** @returns {Promise<void>} Comparte una sola lectura activa y una intención pendiente. */
  drain() {
    if (!this.task) {
      this.task = this.run().finally(
        /** @returns {void} Libera el turno conservando la última intención. */
        () => {
          this.task = null;
          if (this.dirty && this.bound && !this.closed) void this.drain();
        },
      );
    }
    return this.task;
  }

  /** @returns {Promise<void>} Revalida antes de leer y entregar; mantiene errores distintos de listas vacías. */
  async run() {
    try {
      while (this.dirty && this.bound && !this.closed) {
        this.dirty = false;
        const changes = this.changes.splice(0);
        await this.authorize();
        if (this.closed) return;
        const { data: next, cursor } = validatedSnapshot(await this.read(this.query, changes));
        if (this.closed) return;
        await this.authorize();
        if (this.closed) return;
        const operations = windowDelta(this.base, next, this.collections);
        if (!operations.length) continue;
        if (
          operations.length > this.maxOperations ||
          Buffer.byteLength(JSON.stringify(operations)) > this.maxBytes
        ) {
          this.send('resync-required', { reason: 'WINDOW_OVERFLOW' });
          this.bound = false;
          return;
        }
        this.send('patch', {
          baseCursor : this.cursor,
          cursor,
          operations,
        });
        this.base = next;
        this.cursor = cursor;
      }
    } catch (error) {
      if (this.closed) return;
      const revoked = [401, 403, 404].includes(error.statusCode);
      this.send(error.resyncRequired ? 'resync-required' : revoked ? 'revoked' : 'error', {
        descriptionCode : revoked ? 'ACCESS_REVOKED' : 'UNAVAILABLE',
      });
      this.bound = false;
      this.dirty = false;
      if (revoked) this.close();
    }
  }

  /** @param {string} type Tipo de entrega. @param {Object} data Diferencia o control. @returns {void} Emite un sobre estándar sin identidad privada. */
  send(type, data) {
    this.emit({
      type,
      subscriptionId : this.subscriptionId,
      watchVersion   : this.watchVersion,
      topic          : this.topic,
      section        : this.section,
      delivery       : ++this.delivery,
      eventId        : randomUUID(),
      occurredAt     : Date.now(),
      data,
    });
  }

  /** @returns {void} Invalida lecturas en vuelo y libera su base de datos pública. */
  close() {
    if (this.closed) return;
    this.closed = true;
    this.bound = false;
    this.dirty = false;
    this.base = undefined;
    this.query = undefined;
    this.changes = [];
    this.onClose?.();
  }
}

module.exports = { ReadWindow };
