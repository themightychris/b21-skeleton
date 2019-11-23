<?php

Git::$repositories['b21-skeleton'] = [
    'remote' => 'git@git.jarv.us:b21/b21-skeleton.git',
    'originBranch' => 'master',
    'workingBranch' => 'master',
    'trees' => [
        'php-config/Git.config.d/b21-skeleton.php',
        'php-config/Slate/Connectors/DataWarehouse/Connector.config.d',

        'php-classes/Slate/Connectors/DataWarehouse/Connector.php',
        'data-exporters',
        'event-handlers/Slate/CBL/export-data',
        'html-templates/connectors/data-warehouse',
        'site-root/connectors/data-warehouse.php'
    ]
];