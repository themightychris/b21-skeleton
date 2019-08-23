<?php

namespace Slate\CBL;

use ActiveRecord;
use DB;
use Exception;
use Site;

use Slate\People\Student;

use Emergence\Database\PostgresConnection;

class DataWarehouseExporter
{
    public static $schema = 'nafis_test_school';
    public static $exporters = [
        // 'b21-data-warehouse/students' => [
        //     'table' => 'student',
        //     'query' => []
        // ],
        // 'b21-data-warehouse/staff' => [
        //     'table' => 'staff',
        //     'query' => []
        // ],
        // 'slate-cbl/student-portfolios' => [
        //     'table' => 'studentportfolio',
        //     'query' => [],
        //     'headers' => [
        //         'PersonID' => null,
        //         'StudentFullName' => null,
        //         'CompetenciesCount' => null,

        //         'ContentAreaCode' => 'competencyarea',
        //         'Level' => 'portfolio',
        //         'DemonstrationsAverage' => 'performancelevel',
        //         'DemonstrationsRequired' => 'totaler',
        //         'DemonstrationsComplete' => 'completeder',
        //         'DemonstrationsMissed' => 'misseder',
        //         'DemonstrationOpportunities' => 'totalopportunities',
        //     ]
        // ],
        // 'slate-cbl/student-competencies' => [
        //     'table' => 'studentcompetency',
        //     'query' => [],
        //     'headers' => [
        //         'PersonID' => null,
        //         'StudentFullName' => null,

        //         'CompetencyCode' => 'competency',
        //         'Level' => 'portfolio',
        //         'BaselineRating' => 'baseline',
        //         'DemonstrationsAverage' => 'performancelevel',
        //         'DemonstrationsRequired' => 'totaler',
        //         'DemonstrationsComplete' => 'completeder',
        //         'DemonstrationsMissed' => 'misseder',
        //         'DemonstrationOpportunities' => 'totalopportunities'
        //     ]
        // ],
        // 'slate-cbl/student-tasks' => [
        //     'table' => 'studenttask',
        //     'query' => [],
        //     'headers' => [
        //         'StudentFullName' => null,
        //         'CreatorFullName' => null,
        //         'SectionTitle' => null,

        //         'TaskExperienceType' => 'experiencename',
        //         'Status' => 'currentstatusoftask',
        //         'TermTitle' => 'term',
        //         'SkillCodes' => 'skillscodes'

        //         // 'TeacherStaffFK'
        //     ]
        // ],
        // 'slate/terms' => [
        //     'table' => 'term',
        //     'query' => [],
        //     'headers' => [
        //         'Title' => 'term'
        //     ]
        // ],
        'slate-cbl/demonstrations' => [
            'table' => 'studentrating',
            'query' => [],
            'headers' => [
                'ArtifactURL' => 'artifact',
                'Standard' => 'skill',
                'Portfolio' => 'level',
                'PerformanceType' => 'tasktitle',
                'Context' => 'experiencename',
                'Created' => 'timestamp',
                'Level' => 'portfolio',
                'TeacherUsername' => 'teacherstafffk'
            ]
        ]
    ];

    public static function getPdo()
    {
        static $Pdo;

        if ($Pdo === null) {
            $Pdo = PostgresConnection::createInstance([
                'host' => 'db-postgresql-nyc1-64886-do-user-2138858-0.db.ondigitalocean.com',
                'port' => 25060,
                'username' => 'doadmin',
                'password' => 'oueybopukeab7wby',
                'database' => 'test',
                'search_path' => ['nafis_test_school', 'public']
            ]);
        }

        return $Pdo;
    }

    public static function exportData()
    {

        $Pdo = static::getPdo();
        $exporters = static::$exporters;

        DB::suspendQueryLogging();
        // ActiveRecord::$useCache = true;
        set_time_limit(0);

        foreach ($exporters as $scriptName => $scriptCfg) {
            try {
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

                // truncate table
                $Pdo->nonQuery("TRUNCATE ONLY {$scriptCfg['table']} RESTART IDENTITY");

                $results = call_user_func($config['buildRows'], $query, $config);
                foreach ($results as $row) {
                \MICS::dump($row, 'row');

                    foreach($row as $column => $value) {
                        if (!empty($scriptCfg['headers']) && array_key_exists($column, $scriptCfg['headers'])) {
                            if ($scriptCfg['headers'][$column]) {
                                $row[$scriptCfg['headers'][$column]] = $value;
                            }
                        } else {
                            $row[strtolower($column)] = $value;
                        }
                        unset($row[$column]);
                    }
                    $Pdo->insert($scriptCfg['table'], $row);
                }

            } catch (\Exception $e) {
                \MICS::dump($e, 'exception');
                // $failedScripts[] = $scriptNode;
                // continue;
                \MICS::dump($row, 'row');
            }
        }

        DB::resumeQueryLogging();

        // static::respond('');
    }

}