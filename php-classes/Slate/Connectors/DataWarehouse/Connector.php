<?php

namespace Slate\Connectors\DataWarehouse;

use Emergence\Connectors\IJob;
use Psr\Log\LogLevel;
use Slate\CBL\DataWarehouseExporter as DataWarehouse;

class Connector extends \Emergence\Connectors\AbstractConnector implements \Emergence\Connectors\ISynchronize
{
    public static $title = 'Data Warehouse';
    public static $connectorId = 'data-warehouse';

    // workflow implementations
    protected static function _getJobConfig(array $requestData)
    {
        $config = parent::_getJobConfig($requestData);

        $config['Pdo'] = DataWarehouse::getPdo();
        $config['exports'] = $requestData['exports'];

        return $config;
    }

    public static function synchronize(IJob $Job, $pretend = true)
    {
        if ($Job->Status != 'Pending' && $Job->Status != 'Completed') {
            return static::throwError('Cannot execute job, status is not Pending or Complete');
        }

        if (empty($Job->Config['exports'])) {
            return static::throwError('Cannot execute job, at least one export must be selected.');
        }

        $Job->Status = 'Pending';

        if (!$pretend) {
            $Job->save();
        }

        // init results struct
        $results = [];

        try {
            $results['push-exports'] = static::pushExports($Job, $pretend);
            $Job->Status = 'Completed';
        } catch (\Exception $e) {
            $Job->logException($e);
            $Job->Status = 'Failed';
        } finally {
            $Job->Results = $results;

            if (!$pretend) {
                $Job->save();
            }

            return true;
        }
    }

    // task handlers
    public static function pushExports(IJob $Job, $pretend = true)
    {
        //init results struct
        $results = [];

        $configuredExports = DataWarehouse::$exporters;


        // performance enhancers
        DB::suspendQueryLogging();
        ActiveRecord::$useCache = true;
        set_time_limit(0);

        foreach ($Job->Config['exports'] as $exportScript) {
            if (!isset($configuredExports[$exportScript])) {
                throw new Exception("Export script $exportScript does not exist or has not yet been configured");
            }

            $exportScriptConfig = $configuredExports[$exportScript];
            $results[$exportScript] = static::pushExport($Job, $exportScript, $exportScriptConfig, $pretend);
            $backupTables[] = $results['tempTable'];
        }

        DB::resumeQueryLogging();
        DataWarehouse::dropBackupTables($Job->Config['Pdo'], $backupTables);

        return $results;
    }


    public static function pushExport(IJob $Job, $scriptPath, array $scriptConfig = [], $pretend = true)
    {
        $scriptNode = Site::resolvePath("data-exporters/{$scriptPath}.php");

        if (!$scriptNode) {
            throw Exception("Script data-exporters/$scriptPath was not found.");
        }

        // load config
        $exportConfig = include($scriptNode->RealPath);

        // check config
        if (empty($exportConfig['buildRows']) || !is_callable($exportConfig['buildRows'])) {
            throw new Exception("Script data-exporters/$scriptPath does not have a callable buildRows method");
        }

        if (empty($exportConfig['headers']) || !is_array($exportConfig['headers'])) {
            throw new Exception("Script data-exporters/$scriptPath does not have a headers array");
        }

        // read query
        if (is_callable($exportConfig['readQuery'])) {
            $query = call_user_func($exportConfig['readQuery'], $scriptConfig['query'], $exportConfig);
            ksort($query);
        } else {
            $query = [];
        }

        $exportRows = call_user_func($exportConfig['buildRows'], $query, $exportConfig);

        $rowColumns = [];
        $rows = [];

        $Pdo = $Job->Config['Pdo'];

        $tempTable = DataWarehouse::createBackupTableAndCopyData($Pdo, $scriptConfig);
        $Job->log(
            LogLevel::INFO,
            'Created backup table "{backupTableName}" for {tableName}',
            [
                'tableName' => $scriptConfig['table'],
                'backupTableName' => $tempTable
            ]
        );

        DataWarehouse::truncateOriginalTable($Pdo, $scriptConfig);
        $Job->log(
            LogLevel::DEBUG,
            'Truncated original table {tableName}',
            [
                'tableName' => $scriptConfig['table']
            ]
        );

        $results = [
            'exported' => 0,
            'tempTable' => $tempTable
        ];

        foreach ($exportRows as $row) {
            $results['exported']++;

            if (!$pretend) {
                if (empty($rowColumns)) {
                    $rowColumns = DataWarehouse::generateRowColumnsSQL($Pdo, DataWarehouse::translateRowHeaders($row, $scriptConfig));
                }
                $rows[] = DataWarehouse::generateRowSQL($Pdo, DataWarehouse::translateRowHeaders($row, $scriptConfig));
                if (DataWarehouse::$chunkInserts && count($rows) >= DataWarehouse::$chunkInserts) {
                    DataWarehouse::exportRows($Pdo, $scriptConfig, $rowColumns, $rows);
                    $rows = [];
                }
            }
        }

        if (!$pretend) {
            if (count($rows)) {
                DataWarehouse::exportRows($Pdo, $scriptConfig, $rowColumns, $rows);
                $rows = null;
            }
        }

        $Job->log(
            LogLevel::INFO,
            'Exported a total of #{totalRowsExported} to table {tableName}',
            [
                'totalRowsExported' => $results['exported'],
                'tableName' => $scriptConfig['table']
            ]
        );

        return $results;
    }
}