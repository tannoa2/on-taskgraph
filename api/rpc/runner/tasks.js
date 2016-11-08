// Copyright 2016, EMC, Inc.

'use strict';

var injector = require('../../../index.js').injector;
var _ = injector.get('_'); // jshint ignore:line
var Errors = injector.get('Errors');
var taskGraphRunner = require('../../../index.js').taskGraphRunner;

var runTask = function(call) {
    taskGraphRunner.taskRunner.runTaskStream.onNext(call.request);
    return {};
};

var cancelTask = function(call) {
    taskGraphRunner.taskRunner.cancelTaskStream.onNext(call.request);
    return {};
}

module.exports = {
    run: runTask,
    cancel: cancelTask
};
