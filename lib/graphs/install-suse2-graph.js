// Copyright 2016, EMC, Inc.

'use strict';

module.exports = {
    friendlyName: 'Install SUSE',
    injectableName: 'Graph.InstallSUSE',
    options: {
        'install-os': {
            version: null,
            _taskTimeout: 3600000, //1 hour
        },
        'validate-ssh': {
            retries: 10,
            'validate-ssh': {
                retries: 10
            }
        }
    },
    tasks: [
        {
            label: 'set-boot-pxe',
            taskName: 'Task.Obm.Node.PxeBoot',
            ignoreFailure: true
        },
        {
            label: 'reboot',
            taskName: 'Task.Obm.Node.Reboot',
            waitOn: {
                'set-boot-pxe': 'finished'
            }
        },
        {
            label: 'install-os',
            taskName: 'Task.Os.Install.SUSE',
            waitOn: {
                'reboot': 'succeeded'
            }
        },
        {
            label: 'rackhd-callback-notification-wait',
            taskName: 'Task.Wait.Notification',
            waitOn: {
                'install-os': 'succeeded'
            }
        },
        {
            label: 'validate-ssh',
            taskName: 'Task.Ssh.Validation',
            waitOn: {
                'rackhd-callback-notification-wait': 'succeeded'
            }
        }
    ]
};
