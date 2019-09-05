<?php

use Slate\Term;

return [
    'title' => 'Slate Terms',
    'description' => 'Each row represents a term',
    'filename' => 'slate-terms',
    'headers' => [
        'StartDate' => 'Start Date',
        'EndDate' => 'End Date',
        // 'Description',
        'Title' => 'Term',
        'TermType' => 'Term Type'
    ],
    'readQuery' => function (array $input) {
        $query = [];

        return $query;
    },
    'buildRows' => function (array $query = [], array $config = []) {

        // build students list
        $terms = Term::getAll();

        // build Term conditions
        $conditions = [];
        $order = [
            'ID'
        ];

        $conditions = Term::mapConditions($conditions);

        // build rows
        $result = DB::query(
            '
                SELECT Term.*
                    FROM `%s` Term
                    WHERE (%s)
                    ORDER BY %s
            ',
            [
                Term::$tableName,
                count($conditions) ? join(') AND (', $conditions) : 'TRUE',
                implode(',', $order)
            ]
        );

        while ($record = $result->fetch_assoc()) {
            $Term = Term::instantiateRecord($record);

            list($termType, $termTitle) = explode('.', $Term->Handle);

            yield [
                'StartDate' => $Term->StartDate,
                'EndDate' => $Term->EndDate,
                // 'Description' => $Term->Description,
                'Title' => $termTitle,
                'TermType' => $termType
            ];
        }

        $result->free();
    }
];