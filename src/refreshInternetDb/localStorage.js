const fs = require('fs');

const { LOCAL_STORAGE_FILEPATH } = require('../constants');


const getLocalStorageProperty = (propertyName) => _getLocalStorage()[propertyName];

const setLocalStorageProperty = (propertyName, newValue) => {
  const localStorage = _getLocalStorage();

  localStorage[propertyName] = newValue;

  fs.writeFileSync(LOCAL_STORAGE_FILEPATH, JSON.stringify(localStorage));

  return localStorage;
};

const _getLocalStorage = () =>
  fs.existsSync(LOCAL_STORAGE_FILEPATH)
    ? JSON.parse(fs.readFileSync(LOCAL_STORAGE_FILEPATH, 'utf8'))
    : {};

module.exports = {
  getLocalStorageProperty,
  setLocalStorageProperty
};
