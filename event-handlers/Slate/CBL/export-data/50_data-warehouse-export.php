<?php

$Job = \Emergence\Connectors\Job::create([
    'Connector' => \Slate\Connectors\DataWarehouse\Connector::class,
    'Config' => [
        'Pdo' => \Slate\Connectors\DataWarehouse\Connector::getPdo(),
        'exports' => \Slate\Connectors\DataWarehouse\Connector::$exports
    ]
]);

\Slate\Connectors\DataWarehouse\Connector::synchronize($Job, false);