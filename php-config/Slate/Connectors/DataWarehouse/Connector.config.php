<?php

namespace Slate\Connectors\DataWarehouse;

Connector::$postgresHost;
Connector::$postgresPort;
Connector::$postgresUsername;
Connector::$postgresPassword;
Connector::$postgresDatabase;
Connector::$postgresSchema;

$exporters = Connector::$exports;

/* Comment out a line below to disable an export */
Connector::$exports = [
    'slate-cbl/student-portfolios' => $exporters['slate-cbl/student-portfolios'],
    'slate-cbl/student-competencies' => $exporters['slate-cbl/student-competencies'],
    'slate-cbl/student-tasks' => $exporters['slate-cbl/student-tasks'],
    'slate/terms' => $exports['slate/terms'],
    'slate-cbl/demonstrations' => $exporters['slate-cbl/demonstrations']
];