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
        $query = [
            'master-term' => null
        ];

        if (!empty($input['master-term'])) {
            if ($input['master-term'] === 'current') {
                $Term = Slate\Term::getCurrent();
            } elseif ($input['master-term'] === 'current-master') {
                $Term = Slate\Term::getCurrent();
                $Term = $Term ? $Term->getMaster() : null;
            } else {
                $Term = Slate\Term::getByHandle($input['master-term']);
            }

            if (!$Term) {
                throw new RangeException('master-term could not be found');
            }

            $query['master-term'] = $Term;
        }

        return $query;
    },
    'buildRows' => function (array $query = [], array $config = []) {
        // build Term conditions
        $conditions = [];

        if (!empty($query['master-term'])) {
            $MasterTerm = $query['master-term'];
            $conditions['Left'] = [
                'value' => $MasterTerm->Left,
                'operator' => '>='
            ];
            $conditions['Right'] = [
                'value' => $MasterTerm->Right,
                'operator' => '<='
            ];
        }

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