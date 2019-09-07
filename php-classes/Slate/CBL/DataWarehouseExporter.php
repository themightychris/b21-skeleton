<?php

namespace Slate\CBL;

use ActiveRecord;
use Exception;
use DB;
use RequestHandler;
use Site;

use Slate\People\Student;

use Emergence\Database\PostgresConnection;

class DataWarehouseExporter
{
    public static $postgresHost;
    public static $postgresPort;
    public static $postgresUsername;
    public static $postgresPassword;
    public static $postgresDatabase;
    public static $postgresSchema;

    public static $exporters = [
        'slate-cbl/student-portfolios' => [
            'table' => 'studentportfolio',
            'query' => [],
            'headers' => [
                'PersonID' => null,
                'StudentFullName' => null,
                'CompetenciesCount' => null,

                'ContentAreaCode' => 'competencyarea',
                'Level' => 'portfolio',
                'DemonstrationsAverage' => 'performancelevel',
                'DemonstrationsRequired' => 'totaler',
                'DemonstrationsComplete' => 'completeder',
                'DemonstrationsMissed' => 'misseder',
                'DemonstrationOpportunities' => 'totalopportunities',
            ]
        ],
        'slate-cbl/student-competencies' => [
            'table' => 'studentcompetency',
            'query' => [],
            'headers' => [
                'PersonID' => null,
                'StudentFullName' => null,

                'ID' => 'studentcompetencyslatepk',
                'CompetencyCode' => 'competency',
                'Level' => 'portfolio',
                'BaselineRating' => 'baseline',
                'DemonstrationsAverage' => 'performancelevel',
                'DemonstrationsRequired' => 'totaler',
                'DemonstrationsComplete' => 'completeder',
                'DemonstrationsMissed' => 'misseder',
                'DemonstrationOpportunities' => 'totalopportunities'
            ]
        ],
        'slate-cbl/student-tasks' => [
            'table' => 'studenttask',
            'query' => [
                'term' => 'current-master'
            ],
            'headers' => [
                'StudentFullName' => null,
                'TermTitle' => null,

                'ID' => 'studenttaskslatepk',
                'CreatorUsername' => 'teacherstafffk',
                'TaskExperienceType' => 'experiencetype',
                'SectionTitle' => 'experiencename',
                'Status' => 'currentstatusoftask',
                'TermHandle' => 'term',
                'SkillCodes' => 'skillscodes'
            ]
        ],
        'slate/terms' => [
            'table' => 'learningcycle',
            'query' => [],
            'headers' => [
                'Title' => 'term'
            ]
        ],
        'slate-cbl/demonstrations' => [
            'table' => 'studentrating',
            'query' => [
                'term' => 'current-master'
            ],
            'headers' => [
                'StudentID' => null,
                'CreatorFullName' => null,
                'StudentFullName' => null,

                'ID' => 'studentratingslatepk',
                'ArtifactURL' => 'artifact',
                'Standard' => 'skill',
                'Portfolio' => 'level',
                'PerformanceType' => 'tasktitle',
                'Context' => 'experiencename',
                'Level' => 'portfolio',
                'CreatorUsername' => 'teacherstafffk'
            ]
        ]
    ];

    public static function getPdo()
    {
        static $Pdo;

        if ($Pdo === null) {
            $Pdo = PostgresConnection::createInstance([
                'host' => static::$postgresHost,
                'port' => static::$postgresPort,
                'username' => static::$postgresUsername,
                'password' => static::$postgresPassword,
                'database' => static::$postgresDatabase,
                'search_path' => [static::$postgresSchema]
            ]);
        }

        return $Pdo;
    }

    public static function exportData()
    {

        $Pdo = static::getPdo();
        $exporters = static::$exporters;
        $schema = static::$postgresSchema;

        DB::suspendQueryLogging();
        ActiveRecord::$useCache = true;
        set_time_limit(0);

        $renameTables = [];

        try {
            foreach ($exporters as $scriptName => $scriptCfg) {
                $scriptNode = Site::resolvePath("data-exporters/{$scriptName}.php");

                if (!$scriptNode) {
                    throw Exception("Script $scriptPath was not found.");
                }

                // load config
                $config = include($scriptNode->RealPath);

                // check config
                if (empty($config['buildRows']) || !is_callable($config['buildRows'])) {
                    throw new Exception("Script $scriptPath does not have a callable buildRows method");
                }

                if (empty($config['headers']) || !is_array($config['headers'])) {
                    throw new Exception("Script $scriptPath does not have a headers array");
                }

                // read query
                if (is_callable($config['readQuery'])) {
                    $query = call_user_func($config['readQuery'], $scriptCfg['query'], $config);
                    ksort($query);
                } else {
                    $query = [];
                }

                // create temp table and copy data
                $tempTable = $scriptCfg['table'] . '_bak';
                $Pdo->nonQuery("CREATE TABLE IF NOT EXISTS $schema.{$tempTable} (like $schema.{$scriptCfg['table']} including all);");
                $Pdo->nonQuery("INSERT INTO $schema.{$tempTable} SELECT * FROM $schema.{$scriptCfg['table']}");

                $results = call_user_func($config['buildRows'], $query, $config);
                $rows = [];
                foreach ($results as $row) {
                    $copiedRow = [];

                    foreach($row as $column => $value) {
                        if (!empty($scriptCfg['headers']) && array_key_exists($column, $scriptCfg['headers'])) {
                            if ($scriptCfg['headers'][$column]) {
                                $copiedRow[$scriptCfg['headers'][$column]] = $value;
                            }
                        } else {
                            $copiedRow[strtolower($column)] = $value;
                        }
                    }
                    $rows[] = $copiedRow;
                }

                // truncate table, and insert rows
                $Pdo->nonQuery("TRUNCATE TABLE $schema.{$scriptCfg['table']} RESTART IDENTITY;");

                if (!empty($rows)) {
                    $Pdo->insertMultiple($scriptCfg['table'], $rows);
                    unset($rows);
                }

                $renameTables[$scriptCfg['table']] = $tempTable;
            }
        } catch (\Exception $e) {
            return RequestHandler::throwInvalidRequestError('Unable to complete export: '. $e->getMessage());
        } finally {
            DB::resumeQueryLogging();
        }

        foreach ($renameTables as $original => $backup) {
            // drop backup table
            $Pdo->nonQuery("DROP TABLE $schema.$backup");
            // rename table
            // $Pdo->nonQuery("ALTER TABLE $schema.$original RENAME TO $schema.$backup");
        }
    }

}