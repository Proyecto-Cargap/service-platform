/** @file Ventanas Supplier con consultas canónicas, autorización vigente y diferencias por registro. */
'use strict';
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const contract             = require('../../../contracts/service.json').endpoints.find(
  /** @param {Object} endpoint Capacidad. @returns {boolean} Frontera HTTP correlacionada. */
  (endpoint) => endpoint.name === 'realtime-read',
);
/** @param {Object} dependencies Mensajería acreditada. @returns {Object} Registro cerrado de páginas y fichas. */
function supplierWindows({ messaging }) {
  const sections = {};
  for (const request of contract.request.oneOf) {
    if (request.properties.topic.enum[0] !== 'supplier.supplier') continue;
    const section = request.properties.section.enum[0];
    const output  = contract.successData.oneOf.find(
      /** @param {Object} schema Respuesta. @returns {boolean} Mismo tópico y sección de esta consulta. */
      (schema) =>
        schema.properties.control.properties.topic.enum[0] === 'supplier.supplier' &&
        schema.properties.control.properties.section.enum[0] === section,
    ).properties.data;
    sections[section] = {
      collections : section === 'page' ? { '/items': 'supplierCode' } : {},
      /** @param {Object} query Consulta pública. @returns {Object} Límites canónicos sin campos privados ni condiciones arbitrarias. */
      validate(query) {
        if (!validate(query, request.properties.query))
          throw new ApplicationError(400, 'INVALID_SUPPLIER_QUERY');
        return query;
      },
      /** @param {Object} query Consulta. @param {Object} identity Principal revalidado. @returns {Promise<Object>} Base y cursor emitidos por el propietario. */
      async read(query, identity) {
        if (
          !identity.context.policies?.some(
            /** @param {string} policy Facultad. @returns {boolean} Supplier decidirá el ámbito de cada fila. */
            (policy) => policy.startsWith('supplier.supplier.view.'),
          )
        )
          throw new ApplicationError(403, 'FORBIDDEN');
        const response = await messaging.request(
          'service-supplier',
          section === 'page' ? 'supplier-get-list' : 'supplier-get-one',
          query,
          { context: identity.context },
        );
        if (section === 'detail' && response?.statusCode === 404)
          throw new ApplicationError(403, 'SUPPLIER_RESOURCE_UNAVAILABLE');
        if (response?.statusCode !== 200)
          throw new ApplicationError(
            response?.statusCode || 503,
            response?.descriptionCode || 'SUPPLIER_UNAVAILABLE',
          );
        if (!validate(response.data, output))
          throw new ApplicationError(503, 'INVALID_SUPPLIER_PROJECTION');
        return readSnapshot(response);
      },
      /** @param {Object} query Consulta. @param {Object} change Referencia durable. @param {Object} identity Principal. @param {Object} base Ventana autorizada por el servidor. @returns {boolean} Incluye entradas, salidas, reasignaciones y perfiles personales visibles. */
      affects(query, change, identity, base) {
        const rows = base ? (section === 'page' ? base.items : [base]) : [];
        if (change.source === 'directory')
          return (
            change.resource === 'contact' &&
            (!base ||
              rows.some(
                /** @param {Object} row Proveedor autorizado. @returns {boolean} Su identidad personal depende del contacto cambiado. */
                (row) => row.type === 'person' && row.contactCode === change.code,
              ))
          );
        const item = change.item;
        if (!item) return false;
        if (section === 'detail') return query.supplierCode === item.supplierCode;
        return (
          query.scope === 'global' ||
          identity.context.policies?.includes('supplier.supplier.view.global') ||
          item.workspace === identity.workspace ||
          rows.some(
            /** @param {Object} row Fila anterior. @returns {boolean} Reasignación que retira un registro previamente visible. */
            (row) => row.supplierCode === item.supplierCode,
          )
        );
      },
    };
  }
  return { 'supplier.supplier': sections };
}
module.exports = { supplierWindows };
