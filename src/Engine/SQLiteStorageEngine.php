<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Engine;

use vhd\BlazingOrm\Metadata\Field;
use vhd\BlazingOrm\Metadata\Record;

class SQLiteStorageEngine extends AbstractSQLEngine
{

    protected function populateFieldData(Field $field): void
    {
        parent::populateFieldData($field);
        foreach ($field->columns as $column) {
            if (str_starts_with($column->ddl, 'enum(')) {
                $column->ddl = 'varchar(255) NOT NULL';
            } elseif (str_starts_with($column->ddl, 'JSON ')) {
                $column->ddl = 'json NOT NULL';
            }
        }
    }

    public function getTableDDL(Record $record): array
    {
        $sql = $createIndex = [];
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
                    $columns[] = "`{$matches['name']}`"; // SQLite does not support column length
                } else {
                    $columns[] = "`$column`";
                }
            }
            $columns = implode(',', $columns);
            if ($index->ddl) {
                $sql[] = "    " . $index->ddl;
            } elseif ($index === $record->primaryKey) {
                $sql[] = "    constraint PK primary key ($columns)";
            } elseif ($index->unique) {
                $sql[] = "    constraint {$record->table}_$index->name unique ($columns)";
            } else {
                $createIndex[] = "CREATE INDEX {$record->table}_$index->name on $record->table ($columns);";
            }
        }
        $sql[count($sql) - 1] = rtrim($sql[count($sql) - 1], ',');
        $sql[] = ");";
        $sql = implode("\n", $sql);
        return array_merge([$sql], $createIndex);
    }

    protected function upsertRecords(string $class, array $records): void
    {
        $metadata = $this->getRecordMetadata($class);
        $sqlFields = [];
        foreach ($metadata->fields as $field) {
            foreach ($field->columns as $column) {
                $sqlFields[$column->name] = count($sqlFields);
            }
        }
        $paramsPlaceholder = '(' . implode(',', array_fill(0, count($sqlFields), '?')) . ')';

        $chunks = array_chunk($records, $metadata->insertBatchLimit);
        foreach ($chunks as $records) {
            /** @noinspection SqlResolve */
            $pattern = "REPLACE INTO `%s`(`%s`) VALUES %s";
            $sql = sprintf(
                $pattern,
                $metadata->table,
                implode('`,`', array_keys($sqlFields)),
                implode(',', array_fill(0, count($records), $paramsPlaceholder))
            );
            $stmt = $this->connection->prepare($sql);
            $this->bindOrderedPDOParams($stmt, $metadata, $records, $metadata->fields, $sqlFields);
            $stmt->execute();
        }
    }

}