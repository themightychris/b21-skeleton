<?php

$Job = \Emergence\Connectors\Job::create([
    'Connector' => \Slate\Connectors\DataWarehouse\Connector::class,
    'Config' => [
        'Pdo' => \Slate\CBL\DataWarehouseExporter::getPdo(),
        'exports' => [
            'slate/terms',
            'slate-cbl/demonstrations',
            'slate-cbl/student-competencies',
            'slate-cbl/student-portfolios',
            'slate-cbl/student-tasks'
        ]
    ]
]);

\Slate\Connectors\DataWarehouse\Connector::synchronize($Job, false);