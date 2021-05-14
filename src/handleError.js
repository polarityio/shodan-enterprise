const fp = require('lodash/fp');

const STATUS_CODE_ERROR_MESSAGE = {
  400: (error) => ({
    err: error.message,
    detail: `User credentials are not valid, Login Request Failed: ${error.description}`
  }),
  401: (error) => ({
    err: 'Unauthorized',
    detail: 'Unable to retrieve Auth Token -> ' + `${error.description}`
  }),
  404: (error) => ({
    err: 'Not Found',
    detail:
      'Requested item doesn’t exist or not enough access permissions -> ' +
      `${error.description}`
  }),
  429: (error) => ({
    err: 'Too Many Requests',
    detail:
      'Exceeded API Request Limits within a certain timeframe -> ' +
      `${error.description}`
  }),
  500: (error) => ({
    err: 'Server Error',
    detail: 'Unexpected Server Error -> ' + `${error.description}`
  }),
  internalServiceError: (error) => ({
    err: 'Internal Service Error',
    detail: 'Internal Service Error -> ' + `${error.description}`
  }),
  unknown: (error) =>
    error.message.includes('getaddrinfo ENOTFOUND')
      ? {
          err: 'Url Not Found',
          detail: `The Url you used in options was Not Found -> ${error.message}`
        }
      : error.message.includes('self signed certificate')
      ? {
          err: 'Problem with Certificate',
          detail: `A Problem was found with your Certificate -> ${error.message}`
        }
      : {
          err: 'Unknown',
          detail: error.message
        }
};

const handleError = (error) =>
  (
    STATUS_CODE_ERROR_MESSAGE[error.status] ||
    STATUS_CODE_ERROR_MESSAGE[Math.round(error.status / 10) * 10] ||
    STATUS_CODE_ERROR_MESSAGE['unknown']
  )(error);

const checkForInternalServiceError = (statusCode, response) => {
  // TODO: Update custom error handling
  const error = fp.get('error', response);
  if (error) {
    const internalServiceError = Error(error);
    internalServiceError.status = 'internalServiceError';
    internalServiceError.description = error;
    throw internalServiceError;
  }
  return response;
};

module.exports = { handleError, checkForInternalServiceError };
