<?php

Git::$repositories['b21-skeleton'] = [
    'remote' => 'git@git.jarv.us:b21/b21-skeleton.git',
    'originBranch' => 'master',
    'workingBranch' => 'master',
    'trees' => [
        'php-config/Git.config.d/b21-skeleton.php',
        'php-config/Slate/Connectors/DataWarehouse/Connector.config.php',

        'data-exporters',

        'html-templates/connectors/data-warehouse',

        'php-classes/Slate/Connectors/DataWarehouse/Connector.php',

        'site-tasks/exports/data-warehouse-exporter.php',

        'site-root/connectors/data-warehouse.php',

        'site-tasks/exports/data-warehouse-exporter.php',
        'event-handlers/Slate/CBL/export-data'
    ]
];