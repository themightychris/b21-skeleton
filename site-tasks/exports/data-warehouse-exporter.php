<?php

use Slate\CBL\DataWarehouseExporter;

return [
    'title' => 'Export to Data Warehouse',
    'description' => 'Export SLATE data to B21 Postgres Data Warehouse',
    'icon' => 'cloud-upload',
    'handler' => function () {
        if ($_SERVER['REQUEST_METHOD'] == 'POST') {
            $success = DataWarehouseExporter::exportData();

            return static::respond('message', [
                'title' => 'Data Exported',
                'message' => 'Slate CBL Data has been exported to B21 Data Warehouse.'
            ]);
        }

        return static::respond('confirm', [
            'question' => "Export SLATE CBL Data to B21 Data Warehouse?"
        ]);
    }
];