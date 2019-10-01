<?php

Git::$repositories['b21-skeleton'] = [
    'remote' => 'git@git.jarv.us:b21/b21-skeleton.git',
    'originBranch' => 'master',
    'workingBranch' => 'master',
    'trees' => [
        'php-config/Git.config.d/b21-skeleton.php',
        'php-config/Slate/CBL/DataWarehouseExporter.config.php',
        'data-exporters',
        'php-classes/Slate/CBL/DataWarehouseExporter.php',
        'site-tasks/exports/data-warehouse-exporter.php',
        'event-handlers/Slate/CBL/export-data',

        // overrides needed for CLI automation
        'php-classes/ActiveRecord.class.php',
        'php-classes/Slate/UI/Adapters/User.php',
        'php-classes/Slate/CBL/Tasks/StudentTask.php'
    ]
];