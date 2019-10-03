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
            'query' => [
                'master-term' => 'current-master'
            ],
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

    public static $chunkInserts = 5000;

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

    public static function exportRows(PostgresConnection $Pdo, array $scriptCfg, $rowColumns, array $rows)
    {
        $query = '';

        if (!empty($rows)) {
            $query .= 'INSERT INTO ' . $Pdo->quoteIdentifier($scriptCfg['table']);
            $query .= ' ';
            $query .= $rowColumns;
            $query .= ' VALUES ';

            $Pdo->nonQuery($query . implode(', ', $rows));
        }
    }

    public static function translateRowHeaders(array $row, array $scriptCfg)
    {
        $translated = [];
        foreach($row as $column => $value) {
            if (!empty($scriptCfg['headers']) && array_key_exists($column, $scriptCfg['headers'])) {
                if ($scriptCfg['headers'][$column]) {
                    $translated[$scriptCfg['headers'][$column]] = $value;
                }
            } else {
                $translated[strtolower($column)] = $value;
            }
        }

        return $translated;
    }

    public static function generateRowColumnsSQL(PostgresConnection $Pdo, array $record)
    {
        return ' (' . implode(',', array_map([$Pdo, 'quoteIdentifier'], array_keys($record))) . ')';
    }

    public static function generateRowSQL(PostgresConnection $Pdo, array $record)
    {
        return '('. implode(', ', array_map([$Pdo, 'quoteValue'], array_values($record))).')';
    }

    public static function createBackupTableAndCopyData(PostgresConnection $Pdo, array $scriptCfg)
    {
        $schema = static::$postgresSchema;
        // create backup table and copy data
        $tempTable = $scriptCfg['table'] . '_bak';
        $Pdo->nonQuery("CREATE TABLE $schema.{$tempTable} (like $schema.{$scriptCfg['table']} including all);");
        $Pdo->nonQuery("INSERT INTO $schema.{$tempTable} SELECT * FROM $schema.{$scriptCfg['table']}");

        return $tempTable;
    }

    public static function truncateOriginalTable(PostgresConnection $Pdo, array $scriptCfg)
    {
        $schema = static::$postgresSchema;
        // truncate original table, and insert rows
        $Pdo->nonQuery("TRUNCATE TABLE $schema.{$scriptCfg['table']} RESTART IDENTITY;");
    }

    public static function dropBackupTables(PostgresConnection $Pdo, array $backupTables)
    {
        $schema = static::$postgresSchema;
        foreach ($backupTables as $backup) {
            $Pdo->nonQuery("DROP TABLE $schema.$backup");
        }
    }

}