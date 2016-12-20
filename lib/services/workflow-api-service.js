// Copyright 2016, EMC, Inc.

'use strict';

var di = require('di');

module.exports = workflowApiServiceFactory;
di.annotate(workflowApiServiceFactory, new di.Provide('Http.Services.Api.Workflows'));
di.annotate(workflowApiServiceFactory,
    new di.Inject(
        'Protocol.TaskGraphRunner',
        'TaskGraph.Store',
        'Services.Waterline',
        'TaskGraph.TaskGraph',
        'Logger',
        'Errors',
        'Promise',
        'Constants',
        '_',
        'Services.Environment',
        'Services.Lookup',
        'TaskGraph.Runner'
    )
);

function workflowApiServiceFactory(
    taskGraphProtocol,
    taskGraphStore,
    waterline,
    TaskGraph,
    Logger,
    Errors,
    Promise,
    Constants,
    _,
    env,
    lookupService,
    taskGraphRunner
) {
    var logger = Logger.initialize(workflowApiServiceFactory);

    function WorkflowApiService() {

    }
    var globalcreateAndRunGraphAVG =0
    var globalcreateAndRunGraphARR = []

    WorkflowApiService.prototype.createAndRunGraph = function (configuration, nodeId) {
        var self = this;
        var start
        return Promise.try(function () {
                console.time('createAndRunGraph');
                console.time('beforeCreateActiveGraph')
                start = new Date().getTime()
            if (!configuration.name || !_.isString(configuration.name)) {
                throw new Errors.BadRequestError('Graph name is missing or in wrong format');
            }
        })
        .then(function () {
            if (nodeId) {
                return waterline.nodes.needByIdentifier(nodeId)
                .then(function (node) {
                    if (node.sku) {
                        return [node, env.get("config." + configuration.name, configuration.name,
                            [node.sku, Constants.Scope.Global])];
                    }
                    return [node, configuration.name];
                }).spread(function (node, name) {
                    return [
                        self.findGraphDefinitionByName(name),
                        taskGraphStore.findActiveGraphForTarget(node.id),
                        node
                    ];
                });
            } else {
                return [self.findGraphDefinitionByName(configuration.name), null, null];
            }
        })
        .spread(function (definition, activeGraph, node) {
            if (activeGraph) {
                throw new Error("Unable to run multiple task graphs against a single target.");
            }
            var context = configuration.context || {};
            return Promise.resolve().then(function () {
                if (node) {
                    context = _.defaults(context, { target: node.id });
                    return lookupService.nodeIdToProxy(node.id)
                    .catch(function (error) {
                        // allow the proxy lookup to fail since not all nodes
                        // wanting to run a workflow may have an entry
                        logger.error('nodeIdToProxy Lookup', { error: error });
                    });
                } else {
                    return undefined;
                }
            }).then(function (proxy) {
                console.timeEnd('beforeCreateActiveGraph')
                if (proxy) {
                    context.proxy = proxy;
                }
                return self.createActiveGraph(
                        definition, configuration.options, context, configuration.domain, true);
            });
        })
        .tap(console.time.bind(console, 'evaluateGraphStream onNext'))
        .then(function (graph) {
            taskGraphRunner.taskScheduler.evaluateGraphStream.onNext({graphId: graph.instanceId});
            return graph;
        })
        .tap(console.timeEnd.bind(console, 'evaluateGraphStream onNext'))
        .tap( function(){
            var end = new Date().getTime()
            var e = end-start
            var sum = 0
            console.timeEnd('createAndRunGraph')
            globalcreateAndRunGraphARR.push(e)
            if(globalcreateAndRunGraphARR.length == 1000) {
                globalcreateAndRunGraphARR.forEach(function (item) {
                    sum = sum + item
                })
                //console.log("sum on crearteAndRun", sum)
                globalcreateAndRunGraphAVG = sum / globalcreateAndRunGraphARR.length
                console.log("time_array of crearteAndRun: " + globalcreateAndRunGraphARR)
                console.log("time_avg of crearteAndRun: " + globalcreateAndRunGraphAVG)
            }
        })

        .catch(function (err) {
            return err;
        });
    };

    WorkflowApiService.prototype.findGraphDefinitionByName = function (graphName) {
        return taskGraphStore.getGraphDefinitions(graphName)
        .then(function (graph) {
            if (_.isEmpty(graph)) {
                throw new Errors.NotFoundError('Graph definition not found for ' + graphName);
            } else {
                return graph[0];
            }
        });
    };

    WorkflowApiService.prototype.createActiveGraph = function (
            definition, options, context, domain) {
        console.time('create taskActiveGraph');
        console.time('this.createGraph');
        return this.createGraph(definition, options, context, domain)
        .then(function (graph) {

            console.timeEnd('this.createGraph')
            graph._status = Constants.Task.States.Running;
            return graph.persist();
        })
        .tap(console.timeEnd.bind(console, 'create taskActiveGraph'));
    };

    WorkflowApiService.prototype.createGraph = function (definition, options, context, domain) {
        domain = domain || Constants.DefaultTaskDomain;
        return Promise.resolve()
        .then(function () {
            console.time('TaskGraph.create');
            return TaskGraph.create(domain, {
                definition: definition,
                options: options || {},
                context: context
            }).tap(console.timeEnd.bind(console,'TaskGraph.create'))
        })
        .catch(function (error) {
            logger.error('createGraph fails', {
                definition: definition,
                options: options,
                error: error
            });
            if (!error.status) {
                var badRequestError = new Errors.BadRequestError(error.message);
                badRequestError.stack = error.stack;
                throw badRequestError;
            }
            throw error;
        });
    };

    WorkflowApiService.prototype.runTaskGraph = function (graphId, domain) {
        return taskGraphProtocol.runTaskGraph(graphId, domain)
        .catch(function (error) {
            logger.error('Error publishing event to run task graph', {
                error: error,
                graphId: graphId,
                domain: domain
            });
        });
    };

    WorkflowApiService.prototype.cancelTaskGraph = function (graphId) {
        return waterline.graphobjects.needOne({ instanceId: graphId })
        .then(function (workflow) {
            if (!workflow.active()) {
                throw new Errors.TaskCancellationError(
                    graphId + ' is not an active workflow'
                );
            }

            return taskGraphProtocol.cancelTaskGraph(graphId);
        });
    };

    WorkflowApiService.prototype.deleteTaskGraph = function (graphId) {
        // Taskgraph deletion sequence:
        // 1) Get the graph object by ID
        // 2) Check if the returned workflow is running.
        // 3) If it is running, throw an error. Otherwise go on to step 4.
        // 4) Delete the graph object from the task graph store.
        return waterline.graphobjects.needOne({ instanceId: graphId })
        .then(function (workflow) {
            if (workflow.active()) {
                throw new Errors.ForbiddenError(
                    'Forbidden to delete an active workflow ' + graphId);
            }
            return taskGraphStore.deleteGraph(graphId);
        })
        .then(_.first);
    };

    WorkflowApiService.prototype.defineTaskGraph = function (definition) {
        // Do validation before persisting a definition
        return TaskGraph.validateDefinition(Constants.DefaultTaskDomain, { definition: definition })
        .then(function () {
            return taskGraphStore.persistGraphDefinition(definition);
        })
        .then(function (definition) {
            return definition.injectableName;
        })
        .catch(function (error) {
            logger.error('defineTaskGraph fails', {
                definition: definition,
                error: error
            });
            if (!error.status) {
                var badRequestError = new Errors.BadRequestError(error.message);
                badRequestError.stack = error.stack;
                throw badRequestError;
            }
            throw error;
        });
    };

    WorkflowApiService.prototype.defineTask = function (definition) {
        return taskGraphStore.persistTaskDefinition(definition);
    };

    WorkflowApiService.prototype.getWorkflowsTasksByName = function (injectableName) {
        return taskGraphStore.getTaskDefinitions(injectableName);
    };

    WorkflowApiService.prototype.deleteWorkflowsTasksByName = function (injectableName) {
        return taskGraphStore.getTaskDefinitions(injectableName)
            .then(function (task) {
                if (_.isEmpty(task)) {
                    throw new Errors.NotFoundError(
                        'Task definition not found for ' + injectableName
                    );
                } else {
                    return taskGraphStore.deleteTaskByName(injectableName);
                }
            });
    };

    WorkflowApiService.prototype.putWorkflowsTasksByName = function (definition, injectableName) {
        return taskGraphStore.getTaskDefinitions(injectableName)
            .then(function (task) {
                if (_.isEmpty(task)) {
                    throw new Errors.NotFoundError(
                        'Task definition not found for ' + injectableName
                    );
                } else {
                    return taskGraphStore.persistTaskDefinition(definition);
                }
            });
    };

    WorkflowApiService.prototype.getGraphDefinitions = function (injectableName) {
        return taskGraphStore.getGraphDefinitions(injectableName);
    };

    WorkflowApiService.prototype.getTaskDefinitions = function (injectableName) {
        return taskGraphStore.getTaskDefinitions(injectableName);
    };

    WorkflowApiService.prototype.findActiveGraphForTarget = function (target) {
        return waterline.graphobjects.findOne({
            node: target,
            _status: Constants.Task.ActiveStates
        });
    };

    WorkflowApiService.prototype.getWorkflowsByNodeId = function (id, query) {
        var nodeId = ({ node: id });
        var mergedQuery = _.merge({}, nodeId, query);
        return waterline.graphobjects.find(mergedQuery);
    };

    WorkflowApiService.prototype.getAllWorkflows = function (query) {
        return waterline.graphobjects.find(query)
        .catch(function (err) {
            return { err: err }
        });
    };

    WorkflowApiService.prototype.getWorkflowByInstanceId = function (instanceId) {
        return waterline.graphobjects.needOne({ instanceId: instanceId });
    };

    WorkflowApiService.prototype.destroyGraphDefinition = function (injectableName) {
        return taskGraphStore.destroyGraphDefinition(injectableName);
    };

    return new WorkflowApiService();
}
