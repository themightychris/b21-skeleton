<?php

$Job = \Slate\Connectors\Job::create([
    'Connector' => \Slate\Connectors\DataWarehouse\Connector::class,
    'Config' => [
        'exports' => array_keys(\Slate\Connectors\DataWarehouse\Connector::$exports)
    ]
]);

\Slate\Connectors\DataWarehouse\Connector::synchronize($Job, false);