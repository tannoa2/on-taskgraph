// Copyright 2016, EMC Inc.

'use strict';

var injector = require('../../index').injector;
var swagger = injector.get('Http.Services.Swagger');
var controller = swagger.controller;
var templateApiService = injector.get('Http.Services.Api.Templates');

// GET /templates/:name
var templatesGetByName = controller(function(req, res) {
    return templateApiService.templatesGetByName(req, res);
});

module.exports = {
    templatesGetByName: templatesGetByName
};
