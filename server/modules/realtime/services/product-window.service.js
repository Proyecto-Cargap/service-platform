/** @file Ventanas Product: catálogo, variantes, matrices y asignaciones con consultas canónicas autorizadas. */
'use strict';
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const contract             = require('../../../contracts/service.json').endpoints.find(
  /** @param {Object} endpoint Capacidad pública. @returns {boolean} Frontera Gateway. */
  (endpoint) => endpoint.name === 'realtime-read',
);
/** @param {Object} dependencies Transporte con identidad acreditada. @returns {Object} Registro cerrado de ventanas por sección. */
function productWindows({ messaging }) {
  const topics = {};
  for (const request of contract.request.oneOf) {
    const topic = request.properties.topic.enum[0];
    if (!topic.startsWith('product.')) continue;
    const resource = topic.slice('product.'.length),
      section      = request.properties.section.enum[0];
    const action =
      section === 'matrix'
        ? 'product-price-matrix-get-list'
        : section === 'prices'
          ? 'product-item-prices-get-list'
          : section === 'customer'
            ? 'product-customer-price-list-get'
            : `product-${resource}-get-${section === 'page' ? 'list' : 'one'}`;
    const output = contract.successData.oneOf.find(
      /** @param {Object} schema Contrato de salida. @returns {boolean} Misma sección. */
      (schema) =>
        schema.properties.control.properties.topic.enum[0] === topic &&
        schema.properties.control.properties.section.enum[0] === section,
    ).properties.data;
    topics[topic] ||= {};
    topics[topic][section] = {
      collections :
        section === 'matrix'
          ? {
              '/priceLists' : 'code',
              '/items'      : 'variant.code',
            }
          : ['page', 'prices'].includes(section)
            ? { '/items': 'code' }
            : {},
      /** @param {Object} query Consulta. @returns {Object} Consulta cerrada y paginación explícita. */
      validate(query) {
        if (!validate(query, request.properties.query))
          throw new ApplicationError(400, 'INVALID_PRODUCT_QUERY');
        return query;
      },
      /** @param {Object} query Consulta pública. @param {Object} identity Principal vigente. @returns {Promise<Object>} DTO público y cursor del mismo snapshot de Product. */
      async read(query, identity) {
        const scope = resource === 'assignment' ? 'workspace' : 'global';
        if (!identity.context.policies?.includes(`product.${resource}.view.${scope}`))
          throw new ApplicationError(403, 'FORBIDDEN');
        const response = await messaging.request('service-product', action, query, {
          context : identity.context,
        });
        if ((section === 'detail' || resource === 'assignment') && response?.statusCode === 404)
          throw new ApplicationError(403, 'PRODUCT_RESOURCE_UNAVAILABLE');
        if (response?.statusCode !== 200)
          throw new ApplicationError(
            response?.statusCode || 503,
            response?.descriptionCode || 'PRODUCT_UNAVAILABLE',
          );
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_PRODUCT_PROJECTION');
        return readSnapshot(response);
      },
      /** @param {Object} query Consulta. @param {Object} change Referencia atómica del propietario. @param {Object} identity Principal. @param {Object} base Ventana previamente acreditada. @returns {boolean} Limita proyecciones a recursos, relaciones y referencias afectadas. */
      affects(query, change, identity, base) {
        if (change.source === 'customer')
          return (
            resource === 'assignment' &&
            (!base ||
              (query.customerCode || base.customerCode || base.assignment?.customerCode) ===
                change.code)
          );
        if (resource === 'assignment' && change.workspace !== identity.workspace) return false;
        if (section === 'matrix') return ['sku', 'price', 'price-list'].includes(change.resource);
        if (section === 'prices')
          return change.resource === 'item'
            ? change.code === query.code
            : change.resource === 'price-list' ||
                (['price', 'sku'].includes(change.resource) &&
                  (!change.itemCode ||
                    change.itemCode === query.code ||
                    change.previousItemCode === query.code));
        if (section === 'customer')
          return change.resource === 'assignment' && change.customerCode === query.customerCode;
        if (
          resource === 'price' &&
          section === 'page' &&
          ['sku', 'price-list'].includes(change.resource)
        ) {
          const key = change.resource === 'sku' ? 'skuCode' : 'priceListCode';
          return query[key]
            ? query[key] === change.code
            : !base ||
                base.items.some(
                  /** @param {Object} row Precio autorizado. @returns {boolean} La etiqueta deriva de la referencia cambiada. */
                  (row) => row[key] === change.code,
                );
        }
        if (resource !== change.resource) return false;
        if (section === 'detail') return query.code === change.code;
        for (const key of ['itemCode', 'skuCode', 'priceListCode', 'customerCode']) {
          if (
            query[key] &&
            change[key] !== undefined &&
            query[key] !== change[key] &&
            query[key] !== change['previous' + key[0].toUpperCase() + key.slice(1)]
          )
            return false;
        }
        return true;
      },
    };
  }
  return topics;
}
module.exports = { productWindows };
