<?php

$Job = \Emergence\Connectors\Job::create([
    'Connector' => \Slate\Connectors\DataWarehouse\Connector::class,
    'Config' => [
        'exports' => \Slate\Connectors\DataWarehouse\Connector::$exports
    ]
]);

\Slate\Connectors\DataWarehouse\Connector::synchronize($Job, false);