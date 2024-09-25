<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Engine;

use vhd\BlazingOrm\Metadata\Record;

class MySQLStorageEngine extends AbstractSQLEngine
{

    public function getTableDDL(Record $record): array
    {
        $sql = [];
        $sql[] = "CREATE TABLE `$record->table` (";
        foreach ($record->fields as $field) {
            foreach ($field->columns as $column) {
                $sql[] = "    `$column->name` $column->ddl,";
            }
        }
        foreach ($record->indexes as $index) {
            $columns = [];
            $regex = '/^(?<name>\w+)(\((?<len>\d+)\))?/s';
            foreach ($index->columns as $column) {
                if (preg_match($regex, $column, $matches, PREG_UNMATCHED_AS_NULL)) {
                    $columns[] = "`{$matches['name']}`" . ($matches['len'] ? "({$matches['len']})" : '');
                } else {
                    $columns[] = "`$column`";
                }
            }
            $columns = implode(',', $columns);
            if ($index->ddl) {
                $ddl = $index->ddl;
            } elseif ($index === $record->primaryKey) {
                $ddl = "PRIMARY KEY ({$columns})";
            } else {
                $ddl = ($index->unique ? 'UNIQUE ' : '')
                    . "KEY `$index->name` ($columns)";
            }
            $sql[] = "    $ddl,";
        }
        $sql[count($sql) - 1] = rtrim($sql[count($sql) - 1], ',');
        $sql[] = ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
        return [implode("\n", $sql) . ';'];
    }

}