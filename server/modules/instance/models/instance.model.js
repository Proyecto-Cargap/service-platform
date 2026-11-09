/**
 * @file Esquema Mongoose de instancias: campos, valores predeterminados y reglas de persistencia.
 * Define la estructura utilizada al crear y consultar documentos de este recurso.
 */
'use strict';
const mongoose = require('mongoose');

const Schema = mongoose.Schema;

const schema = new Schema({
  /**
   * Espacio de trabajo del registro; allWorkspaces identifica recursos compartidos.
   */
  workspace       : {
    type     : String,
    default  : undefined,
    required : true,
  },
  /**
   * Código del usuario autor; permanece inmutable después de crear el registro.
   */
  userCreate      : {
    type      : String,
    default   : undefined,
    required  : false,
    immutable : true,
  },
  /**
   * Código del usuario responsable; vacío cuando no existe una asignación.
   */
  userAssigned    : {
    type     : String,
    default  : undefined,
    required : false,
  },
  /**
   * Código único de negocio que identifica la instancia.
   */
  instanceCode    : {
    type     : String,
    default  : undefined,
    required : [true, 'instance code not found'],
  },
  /**
   * Código de la sesión vinculada a la instancia.
   */
  sessionCode     : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Dominio de aplicación asociado a la instancia.
   */
  domain          : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Indica si la versión de la instancia coincide con la versión vigente.
   */
  latestVersion   : {
    type     : Boolean,
    default  : false,
    required : false,
  },
  /**
   * Versión y compilación que utiliza la instancia.
   */
  instanceVersion : {
    /**
     * Identificador de la versión del software.
     */
    version : {
      type     : String,
      default  : '',
      required : false,
    },
    /**
     * Identificador de compilación del software.
     */
    build   : {
      type     : String,
      default  : '',
      required : false,
    },
  },
  /**
   * Versión y compilación vigentes al registrar la instancia.
   */
  currentVersion  : {
    /**
     * Identificador de la versión del software.
     */
    version : {
      type     : String,
      default  : '',
      required : false,
    },
    /**
     * Identificador de compilación del software.
     */
    build   : {
      type     : String,
      default  : '',
      required : false,
    },
  },
  /**
   * Identificador de correlación de la operación que produjo el registro.
   */
  idTransaction   : {
    type     : String,
    default  : '',
    required : false,
  },
  /**
   * Marca de baja lógica; su valor no altera automáticamente los filtros de lectura.
   */
  erased          : {
    type     : Boolean,
    default  : false,
    required : false,
  },
  /**
   * Instante de la última modificación del registro, expresado en milisegundos.
   */
  updateDate      : {
    type     : Number,
    default  : Date.now,
    required : false,
  },
  /**
   * Instante de creación del registro, expresado en milisegundos.
   */
  creationDate    : {
    type     : Number,
    /**
     * Ejecuta default dentro del flujo de instance.model.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    default  : function () {
      return new Date().getTime();
    },
    required : false,
  },
});
schema.pre(
  'validate',

  /**
   * Comprueba las condiciones de validateAccessMetadata antes de continuar.
   * @returns {void} Finaliza después de aplicar los efectos de la operación.
   */
  function validateAccessMetadata() {
    for (const field of ['workspace', 'userCreate', 'userAssigned']) {
      if (typeof this[field] !== 'string') this.invalidate(field, 'ACCESS_METADATA_REQUIRED');
    }
  },
);
schema.index({
  workspace  : 1,
  userCreate : 1,
});
schema.index({
  workspace    : 1,
  userAssigned : 1,
});

schema.set('versionKey', false);
schema.set('autoIndex', false);
schema.index({ instanceCode: 1 });
schema.index({ sessionCode: 1 });

module.exports = mongoose.model('PLATFORM_INSTANCE', schema, 'PLATFORM_INSTANCE');
