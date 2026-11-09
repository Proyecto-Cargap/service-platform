/** @file Información canónica para enlaces, correos y QR de los microservicios. */
'use strict';

/**
 * Ejecuta publicURL dentro del flujo de configuration-information.
 * @param {*} value Valor de value utilizado por esta operación.
 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function publicURL(value) {
  const url = new URL(value);
  if (
    url.username ||
    url.password ||
    !(
      url.protocol === 'https:' ||
      (url.protocol === 'http:' && ['localhost', '127.0.0.1'].includes(url.hostname))
    )
  )
    throw new Error('INVALID_PLATFORM_URL');
  return url;
}

/** Valida y proyecta exclusivamente los datos de plataforma necesarios en el servidor.
 * @param {Object} data Datos de entrada de la operación.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function platformInformation(data) {
  if (
    typeof data?.title !== 'string' ||
    !data.title.trim() ||
    data.title.length > 120 ||
    typeof data.publicUrl !== 'string' ||
    typeof data.logos?.email !== 'string' ||
    (data.logos.email && !data.logos.email.trim()) ||
    typeof data.theme?.primary?.['900'] !== 'string' ||
    !/^#[0-9a-f]{6}$/i.test(data.theme?.primary?.['900'] || '')
  )
    throw new Error('INVALID_PLATFORM_INFORMATION');
  const origin = publicURL(data.publicUrl);
  if (origin.origin !== data.publicUrl) throw new Error('PLATFORM_ORIGIN_REQUIRED');
  const logo = data.logos.email ? new URL(data.logos.email, origin) : null;
  if (logo) publicURL(logo.href);
  return {
    title     : data.title,
    publicUrl : origin.origin,
    logos     : { email: logo?.href || '' },
    theme     : { primary: { 900: data.theme.primary['900'] } },
  };
}

module.exports = { platformInformation };
