<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Utility;

use vhd\BlazingOrm\Engine\AbstractSQLEngine;
use vhd\BlazingOrm\Engine\MySQLStorageEngine;
use vhd\BlazingOrm\Engine\SQLiteStorageEngine;

class MigrationHelper
{

    public function getDiffToDatabase(MetadataCollector $metadataCollector, AbstractSQLEngine $engine): array
    {
        $projectDDL = $this->getProjectDDL($metadataCollector, $engine);
        $databaseDDL = $this->getDatabaseDDL($engine);
        if ($engine instanceof MySQLStorageEngine) {
            $diff = $this->generateMySQLDiff($databaseDDL, $projectDDL);
        } elseif ($engine instanceof SQLiteStorageEngine) {
            $diff = $this->generateSQLiteDiff($databaseDDL, $projectDDL);
        } else {
            throw new \RuntimeException('Unsupported engine: ' . get_class($engine));
        }
        return $diff;
    }

    public function getProjectDDL(MetadataCollector $metadataCollector, AbstractSQLEngine $engine): string
    {
        $result = [];
        foreach ($metadataCollector->getRecordClasses() as $recordClass) {
            $recordMeta = $engine->getRecordMetadata($recordClass);
            $result = array_merge($result, $engine->getTableDDL($recordMeta));
        }
        return implode("\n", $result);
    }

    public function getDatabaseDDL(AbstractSQLEngine $engine): string
    {
        if ($engine instanceof MySQLStorageEngine) {
            return $this->getMySQLDatabaseDDL($engine);
        } elseif ($engine instanceof SQLiteStorageEngine) {
            return $this->getSQLiteDatabaseDDL($engine);
        }
        throw new \RuntimeException('Unsupported engine: ' . get_class($engine));
    }

    protected function getMySQLDatabaseDDL(AbstractSQLEngine $engine): string
    {
        $database = $engine->getQuery("SELECT DATABASE()")->fetchCell();
        $sql = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?";
        $tables = $engine->getQuery($sql, $database)->map(fn($TABLE_NAME) => $TABLE_NAME);
        $result = [];
        foreach ($tables as $TABLE_NAME) {
            $result[] = $engine->getQuery("SHOW CREATE TABLE $database.$TABLE_NAME")->fetchRow()['Create Table'];
        }
        return implode(";\n", $result);
    }

    protected function getSQLiteDatabaseDDL(AbstractSQLEngine $engine): string
    {
        $sql = "SELECT name FROM sqlite_master WHERE type='table'";
        $tables = $engine->getQuery($sql)->map(fn($name) => $name);
        $result = [];
        foreach ($tables as $TABLE_NAME) {
            $result[] = $engine->getQuery("SELECT sql FROM sqlite_master WHERE name = ?", $TABLE_NAME)->fetchCell();
            $sql = "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name = ?";
            $indexes = $engine->getQuery($sql, $TABLE_NAME)->map(fn($sql) => $sql);
            $indexes = array_filter($indexes);
            $result = array_merge($result, $indexes);
        }
        return implode(";\n", $result);
    }

    public function generateMySQLDiff(string $currentDDL, string $newDDL): array
    {
        $currentTables = $newTables = [];
        foreach (explode(';', $currentDDL) as $createStatement) {
            if (!trim($createStatement)) {
                continue;
            }
            $ddl = $this->parseMySQLDDL($createStatement);
            $currentTables[$ddl['name']] = $ddl;
        }
        foreach (explode(';', $newDDL) as $createStatement) {
            if (!trim($createStatement)) {
                continue;
            }
            $ddl = $this->parseMySQLDDL($createStatement);
            $newTables[$ddl['name']] = $ddl;
        }

        $alter = [];
        foreach ($newTables as $name => $newTable) {
            if (!isset($currentTables[$name])) {
                $alter[] = $newTable['create'];
                continue;
            }
            $currentTable = $currentTables[$name];
            $newFields = $newTable['fields'];
            $currentFields = $currentTable['fields'];
            $newKeys = $newTable['keys'];
            $currentKeys = $currentTable['keys'];

            $alterTable = [];
            foreach ($newFields as $fieldName => $newField) {
                if (!isset($currentFields[$fieldName])) {
                    $alterTable[] = "ADD COLUMN $newField";
                } elseif ($currentFields[$fieldName] !== $newField) {
                    $alterTable[] = "CHANGE COLUMN `$fieldName` $newField";
                }
            }
            foreach ($currentFields as $fieldName => $currentField) {
                if (!isset($newFields[$fieldName])) {
                    $alterTable[] = "DROP COLUMN $fieldName";
                }
            }
            foreach ($newKeys as $keyName => $newKey) {
                if (!isset($currentKeys[$keyName])) {
                    $alterTable[] = "ADD $newKey";
                } elseif ($currentKeys[$keyName] !== $newKey) {
                    $alterTable[] = "DROP KEY $keyName";
                    $alterTable[] = "ADD $newKey";
                }
            }
            foreach ($currentKeys as $keyName => $currentKey) {
                if (!isset($newKeys[$keyName])) {
                    $alterTable[] = "DROP KEY $keyName";
                }
            }

            if ($alterTable) {
                $alter[] = 'ALTER TABLE ' . $name . "\n" . implode(",\n", $alterTable);
            }
        }

        foreach ($currentTables as $name => $currentTable) {
            if (!isset($newTables[$name])) {
                $alter[] = "DROP TABLE $name";
            }
        }

        return $alter;
    }

    protected function parseMySQLDDL(string $createStatement): array
    {
        preg_match('/CREATE TABLE `(\S+)`\s*\((.+)\)/s', $createStatement, $matches);
        if (count($matches) !== 3) {
            throw new \Exception('No matches in ' . $createStatement);
        }

        $result = [
            'name' => $matches[1],
            'fields' => [],
            'pk' => '',
            'keys' => [],
            'create' => trim($createStatement, ';'),
        ];

        foreach (explode("\n", $matches[2]) as $row) {
            // replace multiple spaces with one
            $row = preg_replace('/\s+/', ' ', $row);
            $row = trim($row, ', ');
            if (!$row) {
                continue;
            }

            $pkRegex = '/PRIMARY KEY \((.+)\)/';
            $keyRegex = '/(KEY|UNIQUE KEY)\s*`(\S+)`\s*\((.+)\)/';

            if (preg_match($pkRegex, $row, $matches)) {
                $result['pk'] = $matches[1];
            } elseif (preg_match($keyRegex, $row, $matches)) {
                $result['keys'][$matches[2]] = $row;
            } elseif (str_starts_with($row, '`')) {
                $name = substr($row, 1, strpos($row, '`', 1) - 1);
                $result['fields'][$name] = $row;
            } else {
                throw new \Exception('Unexpected row: ' . $row);
            }
        }

        return $result;
    }

    public function generateSQLiteDiff(string $currentDDL, string $newDDL): array
    {
        $currentTables = $newTables = [];
        foreach (explode(';', $currentDDL) as $statement) {
            if (trim($statement)) {
                $this->parseSQLiteDDL($currentTables, $statement);
            }
        }
        foreach (explode(';', $newDDL) as $statement) {
            if (trim($statement)) {
                $this->parseSQLiteDDL($newTables, $statement);
            }
        }

        $sql = [];

        foreach ($currentTables as $name => $currentTable) {
            if (!isset($newTables[$name])) {
                $sql[] = "DROP TABLE $name";
            }
        }

        foreach ($newTables as $name => $newTable) {
            if (!isset($currentTables[$name])) {
                $sql[] = $newTable['createDDL'];
                foreach ($newTable['keys'] as $keyName => $key) {
                    $sql[] = "CREATE INDEX $keyName on $name ($key)";
                }
                continue;
            }

            $currentTable = $currentTables[$name];
            if ($newTable['fields'] != $currentTable['fields'] || $newTable['pk'] != $currentTable['pk']) {
                // rebuild
                $tempName = $name . '_tmp_' . time();
                $sql[] = "CREATE TABLE $tempName ({$newTable['bodyDDL']})";

                $fieldList = [];
                $newTableFields = array_keys($newTable['fields']);
                foreach ($newTableFields as $fName) {
                    if (isset($currentTable['fields'][$fName])) {
                        $fieldList[] = $fName;
                    } else {
                        $fieldList[] = "''";
                    }
                }
                $newTableFields = implode(',', $newTableFields);
                $fieldList = implode(',', $fieldList);
                $sql[] = "INSERT INTO $tempName ($newTableFields) SELECT $fieldList FROM $name";
                $sql[] = "DROP TABLE $name";
                $sql[] = "ALTER TABLE $tempName RENAME TO $name";
                foreach ($newTable['keys'] as $keyName => $key) {
                    $sql[] = "CREATE INDEX $keyName on $name ($key)";
                }
                continue;
            }

            $newKeys = $newTable['keys'];
            $currentKeys = $currentTable['keys'];

            foreach ($newKeys as $keyName => $newKey) {
                if (!isset($currentKeys[$keyName])) {
                    $sql[] = "CREATE INDEX $keyName on $name ($newKey)";
                } elseif ($newKey != $currentKeys[$keyName]) {
                    $sql[] = "DROP INDEX $keyName";
                    $sql[] = "CREATE INDEX $keyName on $name ($newKey)";
                }
            }
            foreach ($currentKeys as $keyName => $currentKey) {
                if (!isset($newKeys[$keyName])) {
                    $sql[] = "DROP INDEX $keyName";
                }
            }
        }

        return $sql;
    }

    protected function parseSQLiteDDL(array &$tables, string $statement): void
    {
        if (preg_match('/CREATE TABLE (\S+)\s*\((.+)\)/s', $statement, $matches)) {
            $tableName = trim($matches[1], '`"');
            $tableDef = $tables[$tableName] ?? [];
            $tableDef['name'] = $tableName;
            $tableDef['fields'] = [];
            $tableDef['pk'] = '';
            $tableDef['keys'] ??= [];
            $tableDef['createDDL'] = trim("CREATE TABLE $tableName ($matches[2])");
            $tableDef['bodyDDL'] = $matches[2];

            foreach (explode("\n", $matches[2]) as $row) {
                // replace multiple spaces with one
                $row = preg_replace('/\s+/', ' ', $row);
                $row = trim($row, ', ');
                if (!$row) {
                    continue;
                }

                $pkRegex = '/constraint PK primary key \((.+)\)/';

                if (preg_match($pkRegex, $row, $matches)) {
                    $tableDef['pk'] = $matches[1];
                } elseif (str_starts_with($row, '`')) {
                    $name = substr($row, 1, strpos($row, '`', 1) - 1);
                    $tableDef['fields'][$name] = $row;
                } else {
                    throw new \Exception('Unexpected row: ' . $row);
                }
            }

            $tables[$tableName] = $tableDef;
        } elseif (preg_match('/CREATE INDEX (\S+) on (\S+) \((.+)\)/', $statement, $matches)) {
            $indexName = trim($matches[1], '`"');
            $tableName = trim($matches[2], '`"');
            $tableDef = $tables[$tableName] ?? [];
            $tableDef['keys'][$indexName] = $matches[3];
            $tables[$tableName] = $tableDef;
        } else {
            throw new \Exception('Unexpected statement: ' . $statement);
        }
    }

}