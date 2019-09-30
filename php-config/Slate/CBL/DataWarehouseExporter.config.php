<?php

namespace Slate\CBL;

DataWarehouseExporter::$postgresHost = 'db-postgresql-nyc1-64886-do-user-2138858-0.db.ondigitalocean.com';
DataWarehouseExporter::$postgresPort = 25060;
DataWarehouseExporter::$postgresUsername = 'doadmin';
DataWarehouseExporter::$postgresPassword = 'oueybopukeab7wby';
DataWarehouseExporter::$postgresDatabase = 'test';
DataWarehouseExporter::$postgresSchema = 'nafis_test_school';

$exporters = DataWarehouseExporter::$exporters;

DataWarehouseExporter::$exporters = [
    // 'slate-cbl/student-portfolios' => $exporters['slate-cbl/student-portfolios'],
    // 'slate-cbl/student-competencies' => $exporters['slate-cbl/student-competencies'],
    // 'slate-cbl/student-tasks' => $exporters['slate-cbl/student-tasks'],
    'slate/terms' => $exporters['slate/terms'],
    // 'slate-cbl/demonstrations' => $exporters['slate-cbl/demonstrations']
];