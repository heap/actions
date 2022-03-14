import * as req from "request-promise-native"
import * as semver from "semver"
import * as winston from "winston"

import * as Hub from "../../hub"

export type HeapPropertyType = "user" | "account"
export const HEAP_PROPERTY_TYPES: { [name: string]: HeapPropertyType } = {
  User: "user",
  Account: "account",
}

// HeapFields enumerates supported identifiers for each endpoint
// - "identity" is the user identifier for the user properties export
// - "account_id" is the account identifier for the account properties export
export enum HeapFields {
  Identity = "identity",
  AccountId = "account_id",
}

export type HeapField = HeapFields.Identity | HeapFields.AccountId

const logger = new winston.Logger()
let logLevel = process.env.LOG_LEVEL
if (!logLevel) {
  logLevel = "info"
}
logger.add(winston.transports.Console, {
  json: true,
  stringify: (obj) => JSON.stringify(obj),
  colorize: false,
  timestamp: true,
  level: logLevel,
})

interface LookerFieldMap {
  [fieldName: string]: Hub.Field
}

interface LogTag {
  envId: string
  webhookId: string
}

interface PropertyMap {
  [property: string]: string
}

interface HeapEntity {
  heapFieldValue: string
  properties: PropertyMap
}

export class HeapAction extends Hub.Action {
  static ADD_USER_PROPERTIES_URL =
    "https://heapanalytics.com/api/integrations/add_user_properties"
  static ADD_ACCOUNT_PROPERTIES_URL =
    "https://heapanalytics.com/api/add_account_properties"
  static HEAP_TRACK_URL = "https://heapanalytics.com/api/track"
  static HEAP_LIBRARY = "looker"
  static ROWS_PER_BATCH = 1000
  // TODO: remove me before GA
  static HEAP_ENV_ID = process.env.HEAP_ENV_ID
  static HEAP_IDENTITY = process.env.HEAP_IDENTITY
  static HEAP_EVENT_NAME = "Submit Looker Action"
  static LOG_PROGRESS_STEP = 10000
  static DISPLAY_ERROR_COUNT = 250
  static EMPTY_HEAP_ENTITY = { heapFieldValue: undefined, properties: {} }
  static SUPPORTED_PROPERTY_TYPES: string[] = [HEAP_PROPERTY_TYPES.User, HEAP_PROPERTY_TYPES.Account]

  description = "Add user and account properties to your Heap dataset"
  label = "Heap"
  iconName = "heap/heap.svg"
  name = "heap"
  params = []
  supportedActionTypes = [Hub.ActionType.Query]
  usesStreaming = true
  supportedFormats = (request: Hub.ActionRequest) => {
    if (request.lookerVersion && semver.gte(request.lookerVersion, "6.2.0")) {
      return [Hub.ActionFormat.JsonDetailLiteStream]
    } else {
      return [Hub.ActionFormat.JsonDetail]
    }
  }

  async execute(request: Hub.ActionRequest): Promise<Hub.ActionResponse> {
    try {
      this.validateParams(request.formParams)
    } catch (validationError) {
      logger.error(
        `Heap action failed with an error`,
        {
          err: validationError,
          webhookId: request.webhookId,
          ...request.formParams,
        },
      )
      return new Hub.ActionResponse({
        success: false,
        validationErrors: [validationError],
        message: validationError.message,
      })
    }

    const propertyType: HeapPropertyType = request.formParams
      .property_type as HeapPropertyType
    const envId = request.formParams.env_id!
    const heapFieldLabel: string = request.formParams.heap_field!
    const webhookId = request.webhookId !== undefined ? request.webhookId : "unknown"
    const logTag: LogTag = {
      envId,
      webhookId,
    }
    let identityField: Hub.Field

    let fieldMap: LookerFieldMap = {} as LookerFieldMap
    let heapField: HeapField
    let requestUrl: string
    try {
      heapField = this.resolveHeapField(propertyType, logTag)
      requestUrl = this.resolveApiEndpoint(propertyType, logTag)
    } catch (err) {
      logger.error("Error in preflight resolvers", { ...logTag, err })
      return new Hub.ActionResponse({
        success: false,
        message: err.message,
      })
    }
    const syncErrors: Error[] = []
    const requestPromises: Promise<void>[] = []
    let rowsProcessed = 0
    let rowsReceived = 0
    let requestBatch: HeapEntity[] = []

    try {
      await request.streamJsonDetail({
        onFields: (fieldset) => {
          try {
            logger.debug(`envId ${envId} fieldset ${JSON.stringify(fieldset)}`, logTag)
            const allFields = Hub.allFields(fieldset)
            logger.debug(`envId ${envId} allFields ${JSON.stringify(allFields)}`, logTag)
            identityField = this.extractHeapFieldByLabel(allFields, heapFieldLabel)
            logger.debug(`envId ${envId} identityField`, { ...logTag, identityField, heapFieldLabel })
            fieldMap = this.extractFieldMap(allFields)
            logger.debug(`envId ${envId} fieldMap ${JSON.stringify(fieldMap)}`, logTag)
          } catch (err) {
            logger.error("Encountered errors in processing fields", { ...logTag, err })
            syncErrors.push(err)
          }
        },
        onRow: (row) => {
          if (rowsReceived % HeapAction.LOG_PROGRESS_STEP === 0) {
            logger.info(`Example row for envId ${envId} ${JSON.stringify(row)}`, logTag)
          }
          rowsReceived += 1
          try {
            const { heapFieldValue, properties } = this.extractPropertiesFromRow(
              row,
              identityField,
              fieldMap,
              logTag,
            )
            if (!heapFieldValue) {
              // if heapFieldValue or heapFieldValue is empty, onRow ends early
              return
            }
            rowsProcessed += 1
            requestBatch.push({ heapFieldValue, properties })
            if (requestBatch.length >= HeapAction.ROWS_PER_BATCH) {
              const length = requestBatch.length
              logger.info(`Loading ${length} rows of data to heap`, logTag)
              requestPromises.push(
                this.sendRequest(
                  requestBatch,
                  envId,
                  requestUrl,
                  heapField,
                  logTag,
                  syncErrors,
                ),
              )
              requestBatch = []

              if (rowsProcessed % HeapAction.LOG_PROGRESS_STEP === 0) {
                logger.info(
                  `Processed ${rowsProcessed} rows in ${
                    rowsProcessed / HeapAction.ROWS_PER_BATCH
                  } batch requests for envId ${envId}.`,
                  logTag)
              }
            }
          } catch (err) {
            logger.error("Encountered an error onRow", { ...logTag, err })
            syncErrors.push(err)
          }
        },
      })
    } catch (err) {
      logger.error("Error in request.streamJsonDetail", { err, ...logTag })
      syncErrors.push(err)
    }

    try {
      let length = requestBatch.length
      if (length > 0) {
        winston.info(`Loading the remaining ${length} rows of data to heap`, logTag)
        const promise = this.sendRequest(requestBatch, envId, requestUrl, heapField, logTag, syncErrors)
        requestPromises.push(promise)
      }
      length = requestPromises.length
      logger.info(`Confirming all ${length} requests are resolved`, logTag)
      await Promise.all(requestPromises)
    } catch (err) {
      logger.error("Encountered an error in executing promises", { ...logTag, err })
      syncErrors.push(err)
    }

    try {
      await this.trackLookerAction(
        envId,
        rowsProcessed,
        heapField,
        webhookId,
        syncErrors.length === 0 ? "success" : "failure",
      )
    } catch (err) {
      logger.error("Heap track call failed.", { ...logTag, err })
      syncErrors.push(err)
    }

    if (syncErrors.length === 0) {
      return new Hub.ActionResponse({ success: true })
    } else {
      // limit error message to the first N to avoid returning enormous error messages
      // (arbitrary limit)
      const errorsToDisplay = syncErrors.slice(0, HeapAction.DISPLAY_ERROR_COUNT)
      // tell how many errors there were in total since we're only displaying the first N
      const errorDesc = `Heap action for envId ${envId} failed with ${syncErrors.length} errors`
      if (syncErrors.length > HeapAction.DISPLAY_ERROR_COUNT) {
        logger.error(`${errorDesc} (displaying first ${HeapAction.DISPLAY_ERROR_COUNT})`, logTag)
      } else {
        logger.error(errorDesc, logTag)
      }
      // log first N errors
      errorsToDisplay.forEach((err) => logger.error(`envId ${envId} error: ${err.message}`, { ...logTag, err }))
      // concat first N errors into a signle errorMsg to return to the looker action hub.
      const errorMsg = errorsToDisplay.map((err) => err.message).join(", ")
      return new Hub.ActionResponse({ success: false, message: `${errorDesc} - ${errorMsg}` })
    }
  }

  /*
  * define the instance properties for the connection to Heap. Form.fields will generate UI in looker.
  * Here we defined 3 fields:
  *   - a text box for env_id
  *   - a dropdown for property_type
  *   - a text box for heap_field
  */
  async form() {
    const form = new Hub.ActionForm()
    form.fields = [
      {
        label: "Heap Environment ID",
        name: "env_id",
        required: true,
        type: "string",
      },
      {
        label: "Property Type",
        name: "property_type",
        required: true,
        options: [
          { name: HEAP_PROPERTY_TYPES.Account, label: "Account" },
          { name: HEAP_PROPERTY_TYPES.User, label: "User" },
        ],
        type: "select",
      },
      {
        label:
          "Column name matching user join key or account ID property in Heap",
        name: "heap_field",
        required: true,
        type: "string",
      },
    ]
    return form
  }

  /*
  * validate the connection configuration, it's an input from the looker admin.
  * The validation will be run when a field is received (for backward compatibility)
  * The same validation will be when the connection is initially setup.
  */
  private validateParams(formParams: Hub.ParamMap) {
    if (!formParams.env_id || formParams.env_id.match(/\D/g)) {
      const message = `Heap environment ID is invalid: ${formParams.env_id}`
      throw {
        field: "env_id",
        message,
      }
    }

    if (
      !formParams.property_type ||
      !(HeapAction.SUPPORTED_PROPERTY_TYPES).includes(
        formParams.property_type,
      )
    ) {
      const message = `Unsupported property type: ${formParams.property_type}`
      throw {
        field: "property_type",
        message,
      }
    }

    if (
      !formParams.heap_field ||
      formParams.heap_field.length === 0
    ) {
      const message = "Column mapping to a Heap field must be provided."
      throw {
        field: "heap_field",
        message,
      }
    }
  }

  private extractHeapFieldByLabel(fields: Hub.Field[], heapFieldLabel: string): Hub.Field {
    const heapField = fields.find((field) => field.label === heapFieldLabel)
    if (!heapField) {
      throw new Error(
        `Heap field (${heapFieldLabel}) is missing in the query result.`,
      )
    }
    return heapField
  }

  // we add all labels that might be used as keys for the rows
  private extractFieldMap(allFields: Hub.Field[]): LookerFieldMap {
    return allFields.reduce((fieldMap: LookerFieldMap, field: Hub.Field) => {
      if (field.field_group_label) {
        fieldMap[field.field_group_label] = field
      }
      if (field.label_short) {
        fieldMap[field.label_short] = field
      }
      const fieldName = field.name
      fieldMap[fieldName] = field
      return fieldMap
    }, {} as LookerFieldMap)
  }

  private resolveHeapField(propertyType: HeapPropertyType, logTag: LogTag): HeapField {
    switch (propertyType) {
      case HEAP_PROPERTY_TYPES.Account:
        return HeapFields.AccountId
      case HEAP_PROPERTY_TYPES.User:
        return HeapFields.Identity
      default:
        const error = new Error(`Unsupported property type: ${propertyType}`)
        logger.error(error.message, logTag)
        throw error
    }
  }

  private resolveApiEndpoint(propertyType: HeapPropertyType, logTag: LogTag): string {
    switch (propertyType) {
      case HEAP_PROPERTY_TYPES.User:
        return HeapAction.ADD_USER_PROPERTIES_URL
      case HEAP_PROPERTY_TYPES.Account:
        return HeapAction.ADD_ACCOUNT_PROPERTIES_URL
      default:
        const error = new Error(`Unsupported property type: ${propertyType}`)
        logger.error(error.message, logTag)
        throw error
    }
  }

  private extractRequiredField(
    row: Hub.JsonDetail.Row,
    field: Hub.Field,
  ): {
    key: string,
    value: string,
  } {
    // we have observed different behavior based on how the view is created
    // each of the following could potentially be used as keys of the field,
    // so we check each one until we find one that exists.
    const propertiesToCheck = [field.name, field.label_short, field.field_group_label]
    const fieldName = propertiesToCheck.find((f) => !!f && row.hasOwnProperty(f))
    if (fieldName && !!row[fieldName].value) {
      return {
        key: fieldName,
        value: row[fieldName].value.toString(),
      }
    } else {
      const heapFieldDesc = `${JSON.stringify(propertiesToCheck)}`
      throw new Error(`Found a row without the ${heapFieldDesc} field or the value is empty. ` +
        `row: ${JSON.stringify(row)}`)
    }
  }

  /*
  * transform a row to a HeapEntity
  * if the identity field is not found, skip the value.
  */
  private extractPropertiesFromRow(
    row: Hub.JsonDetail.Row,
    identityField: Hub.Field,
    allFieldMap: LookerFieldMap,
    logTag: LogTag,
  ): {
    heapFieldValue: string | undefined;
    properties: PropertyMap;
  } {
    try {
      const identityFieldValue = this.extractRequiredField(row, identityField)
      const heapFieldValue = identityFieldValue.value

      const properties: { [K in string]: string } = {}
      for (const [fieldName, cell] of Object.entries(row)) {
        if (fieldName !== identityFieldValue.key) {
          const field = allFieldMap[fieldName]
          // Field labels are the original name of the property that has not been sanitized or snake-cased.
          const propertyName =
            field.label !== undefined ? field.label : fieldName
          const cellValue = !!cell.value ? cell.value : cell.filterable_value
          if (!!cellValue) {
            const propertyValue = cellValue.toString().substring(0, 1024)
            // Certain number formats are displayed with commas
            const sanitizedPropertyValue = field.is_numeric
              ? propertyValue.replace(/[^0-9\.]+/g, "")
              : propertyValue
            properties[propertyName] = sanitizedPropertyValue
          }
        }
      }
      return { properties, heapFieldValue }
    } catch (err) {
      logger.error("Encountered an error in heapify, skip the row", {
        ...logTag,
        err,
        row,
      })
      return HeapAction.EMPTY_HEAP_ENTITY
    }
  }

  private constructBodyForRequest(
    appId: string,
    heapField: HeapField,
    heapEntities: HeapEntity[],
  ): string {
    const baseRequestBody = { app_id: appId, library: HeapAction.HEAP_LIBRARY }
    let jsonBody = {}
    if (heapField === HeapFields.Identity) {
      jsonBody = {
        users: heapEntities.map(({ heapFieldValue, properties }) => ({
          user_identifier: { email: heapFieldValue },
          properties,
        })),
      }
    } else if (heapField === HeapFields.AccountId) {
      jsonBody = {
        accounts: heapEntities.map(({ heapFieldValue, properties }) => ({
          account_id: heapFieldValue,
          properties,
        })),
      }
    }
    return JSON.stringify(Object.assign({}, baseRequestBody, jsonBody))
  }

  private async sendRequest(
    requestBatch: HeapEntity[],
    envId: string,
    requestUrl: string,
    heapField: HeapField,
    logTag: LogTag,
    errors: Error[],
  ): Promise<void> {
    const requestBody = this.constructBodyForRequest(
      envId,
      heapField,
      requestBatch,
    )
    try {
      await req
        .post({
          uri: requestUrl,
          headers: { "Content-Type": "application/json" },
          body: requestBody,
        })
        .promise()
    } catch (err) {
      const errorMsg = `Encountered an error in sending request to heap`
      logger.error(errorMsg, { ...logTag, err })
      errors.push(err)
    }
  }

  /**
   * REMOVE ME before GA. Send a track call to heap to summarize the attempted looker action request.
   * @param envId
   * @param recordCount the number of records that were processed by this action.
   * @param heapField identity or account id
   * @param state success if all records were processed correctly and error if any errors were encountered.
   */
  private async trackLookerAction(
    envId: string,
    recordCount: number,
    heapField: HeapField,
    webhookId: string,
    state: "success" | "failure",
  ): Promise<void> {
    const now = new Date().toISOString()
    const requestBody = {
      app_id: HeapAction.HEAP_ENV_ID,
      identity: HeapAction.HEAP_IDENTITY,
      event: HeapAction.HEAP_EVENT_NAME,
      timestamp: now,
      properties: {
        customer_env_id: envId,
        record_count: recordCount,
        field_type: heapField,
        webhookId,
        state,
      },
    }

    try {
      await req
        .post({
          uri: HeapAction.HEAP_TRACK_URL,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(requestBody),
        })
        .promise()
    } catch (err) {
      logger.error(`Encountered an error in trackLookerAction`, { envId, webhookId, err })
      // swallow any errors in the track call
      return
    }
  }
}

Hub.addAction(new HeapAction())
