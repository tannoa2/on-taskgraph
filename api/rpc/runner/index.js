// Copyright 2016, EMC, Inc.

'use strict';

var di = require('di');

module.exports = runnerServerFactory;
di.annotate(runnerServerFactory, new di.Provide('TaskGraph.TaskRunner.Server'));
di.annotate(runnerServerFactory,
    new di.Inject(
        'Assert',
        '_',
        'Promise'
    )
);

function runnerServerFactory(
    assert,
    _,
    Promise
) {
    var grpc = require('grpc');

    function RunnerServer(options) {
        this.options = options || {
            hostname: '0.0.0.0'
        };
        this.options.protoFile = this.options.protoFile || __dirname + '/../../../runner.proto';

        assert.string(this.options.hostname);
        assert.string(this.options.protoFile);
    }

    RunnerServer.prototype.start = function() {
        var self = this;

        return Promise.try(function() {
            var runnerProto = grpc.load(self.options.protoFile).runner;

            var tasks = require('./tasks.js');

            self.gRPC = new grpc.Server();
            self.gRPC.addProtoService(runnerProto.Runner.service, {
                runTask: grpcWrapper(tasks.run),
                cancelTask: grpcWrapper(tasks.cancel)
            });

            self.options.port = self.gRPC.bind(
                self.options.hostname + (self.options.port ? ':' + self.options.port : ''),
                grpc.ServerCredentials.createInsecure());
            self.gRPC.start();
            console.log('gRPC is available on grpc://' + self.options.hostname + ':' + self.options.port);
        });
    }

    RunnerServer.prototype.stop = function() {
        var self = this;
        return Promise.try(function() {
            self.gRPC.forceShutdown();
        });
    }

    function grpcWrapper(rpcEntry) {
        return function(call, callback) {
            return Promise.try(function() {
                return rpcEntry(call, callback);
            })
            .then(function(response) {
                if(response) {
                    callback(null, {
                        response: JSON.stringify(response)
                    });
                }
            })
            .catch(function(err) {
                callback(err);
            })
        };
    }

    return RunnerServer;
}


