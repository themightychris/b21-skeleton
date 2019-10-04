<?php

Git::$repositories['b21-skeleton'] = [
    'remote' => 'git@git.jarv.us:b21/b21-skeleton.git',
    'originBranch' => 'master',
    'workingBranch' => 'master',
    'trees' => [
        'php-config/Git.config.d/b21-skeleton.php',

        'data-exporters',

        'event-handlers/Slate/CBL/export-data',

        'html-templates/connectors/data-warehouse',

        'php-classes/Slate/Connectors/DataWarehouse/Connector.php',

        'site-tasks/exports/data-warehouse-exporter.php',

        'site-root/connectors/data-warehouse.php',

        // overrides needed for CLI automation
        'php-classes/ActiveRecord.class.php',
        'php-classes/Slate/UI/Adapters/User.php',
        'php-classes/Slate/CBL/Tasks/StudentTask.php'
    ]
];