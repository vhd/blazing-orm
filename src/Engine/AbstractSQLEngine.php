<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Engine;

use BackedEnum;
use Closure;
use DateTimeInterface;
use Error;
use JetBrains\PhpStorm\ArrayShape;
use JsonSerializable;
use PDO;
use PDOStatement;
use UnexpectedValueException;
use UnitEnum;
use vhd\BlazingOrm\Metadata\Column;
use vhd\BlazingOrm\Metadata\Field;
use vhd\BlazingOrm\Metadata\Record;
use vhd\BlazingOrm\RecordManagerInterface;
use vhd\BlazingOrm\StorageEngineInterface;
use vhd\BlazingOrm\SyncData;
use vhd\BlazingOrm\Utility\AttributeReader;
use vhd\BlazingOrm\Utility\DateUtil;

abstract class AbstractSQLEngine implements StorageEngineInterface
{

    public const string EMPTY_DATE = '0001-01-01 00:00:00';

    protected const string SUFFIX_TYPE = '_t';
    protected const string SUFFIX_STRING = '_s';
    protected const string SUFFIX_NUMBER = '_n';
    protected const string SUFFIX_BOOLEAN = '_b';
    protected const string SUFFIX_DATETIME = '_d';
    protected const string SUFFIX_ENUM = '_e';
    protected const string SUFFIX_REFERENCE = '_r';
    protected const string SUFFIX_JSON = '_json';

    protected const string ALIAS_NULL = 'null';
    protected const string ALIAS_STRING = 'string';
    protected const string ALIAS_NUMERIC = 'numeric';
    protected const string ALIAS_BOOLEAN = 'boolean';
    protected const string ALIAS_DATETIME = 'datetime';

    protected const string KEY_TYPE = 'binary(16)';
    protected const string EMPTY_KEY = "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

    protected array $typeInfoCache = [];
    protected array $metadataCache = [];

    public function __construct(
        protected PDOWrapper $connection,
        protected RecordManagerInterface $recordManager,
        protected ?AttributeReader $attributeReader = null,
    ) {
        $this->attributeReader ??= AttributeReader::getInstance();
    }

    protected function populateFieldData(Field $field): void
    {
        $fieldName = $field->name;
        $types = $enumValues = [];
        $isFloat = false;
        foreach ($field->phpTypes as $typeName) {
            $typeInfo = $this->getTypeInfo($typeName);
            $types[] = $typeInfo['type'];
            if (is_subclass_of($typeName, UnitEnum::class) && !is_subclass_of($typeName, BackedEnum::class)) {
                /** @var $typeName UnitEnum $e */
                foreach ($typeName::cases() as $e) {
                    $enumValues[] = $e->name;
                }
            } elseif ($typeName === 'float') {
                $isFloat = true;
            }
        }
        unset($typeName);

        $uniqTypes = array_unique($types);
        assert(count($uniqTypes) === count($types));
        $isComposite = count($types) > 1;

        $field->columns = [];

        $enumValues = array_unique($enumValues);
        if ($isComposite) {
            $columnName = $this->getColumnName($fieldName, self::SUFFIX_TYPE);
            $column = new Column(
                name: $columnName,
                ddl: "enum('" . implode("','", $types) . "') NOT NULL",
            );
            $field->columns[$columnName] = $column;
        }

        foreach ($field->phpTypes as $typeName) {
            /** @var string $typeName */
            $typeInfo = $this->getTypeInfo($typeName);
            $suffix = $typeInfo['suffix'];
            $columnName = $this->getColumnName($fieldName, ($isComposite ? $suffix : null));
            if (!isset($field->columns[$columnName])) {
                if ($suffix === self::SUFFIX_STRING) {
                    if ($field->typeHint === 'blob') {
                        $ddl = "blob";
                    } elseif ($field->typeHint === 'mediumblob') {
                        $ddl = "mediumblob";
                    } elseif ($field->typeHint === 'text') {
                        $ddl = "text";
                    } elseif ($field->typeHint === 'mediumtext') {
                        $ddl = "mediumtext";
                    } elseif ($field->typeHint === 'binary') {
                        $ddl = "binary($field->length)";
                    } elseif ($field->typeHint === 'varbinary') {
                        $ddl = "varbinary($field->length)";
                    } else {
                        $ddl = "varchar($field->length)";
                    }
                    $defaultValue = '';
                } elseif ($suffix === self::SUFFIX_NUMBER) {
                    if ($field->decimalDigits) {
                        $ddl = "decimal($field->decimalDigits," . ($field->decimalFractionDigits ?? '0') . ")";
                    } elseif ($isFloat) {
                        $ddl = 'float';
                    } elseif ($field->typeHint === 'bigint') {
                        $ddl = 'bigint';
                    } elseif ($field->typeHint === 'tinyint') {
                        $ddl = 'tinyint';
                    } else {
                        $ddl = 'int';
                    }
                    $defaultValue = 0;
                } elseif ($suffix === self::SUFFIX_BOOLEAN) {
                    $ddl = "tinyint";
                    $defaultValue = 0;
                } elseif ($suffix === self::SUFFIX_DATETIME) {
                    $ddl = $field->typeHint === 'timestamp' ? "timestamp" : "datetime";
                    $defaultValue = static::EMPTY_DATE;
                } elseif ($suffix === self::SUFFIX_ENUM) {
                    if ($isComposite) {
                        array_unshift($enumValues, '');
                    }
                    $ddl = "enum('" . implode("','", $enumValues) . "')";
                    $defaultValue = $enumValues[0];
                } elseif ($suffix === self::SUFFIX_REFERENCE) {
                    $ddl = self::KEY_TYPE;
                    $defaultValue = self::EMPTY_KEY;
                } elseif ($suffix === self::SUFFIX_JSON) {
                    $ddl = "json";
                    $defaultValue = 'null';
                } else {
                    throw new Error();
                }
                $ddl .= " NOT NULL";
                $column = new Column(
                    name: $columnName,
                    ddl: $ddl,
                    defaultValue: $defaultValue,
                    storedTypes: [],
                );
                $field->columns[$columnName] = $column;
            }
            if ($suffix !== self::SUFFIX_TYPE) {
                $field->columns[$columnName]->storedTypes[] = $typeName;
            }
        }
    }

    protected function getColumnName(string $fieldName, ?string $compositeSuffix = null): string
    {
        return $fieldName . $compositeSuffix;
    }

    #[ArrayShape([
        'type' => "string",
        'suffix' => "string",
        'castFn' => "callable",
        'unCastFn' => "callable"
    ])]
    protected function getTypeInfo(string $type): array
    {
        if (isset($this->typeInfoCache[$type])) {
            return $this->typeInfoCache[$type];
        }
        $rm = $this->recordManager;
        if ($type === 'null') {
            $alias = self::ALIAS_NULL;
            $suffix = self::SUFFIX_TYPE;
            $unCastFn = static fn(string|null $v) => null;
            $castFn = static fn() => [null, PDO::PARAM_NULL];
        } elseif ($type === 'string') {
            $alias = self::ALIAS_STRING;
            $suffix = self::SUFFIX_STRING;
            $unCastFn = static fn(string|null $v) => (string)$v;
            $castFn = static fn(string $v) => [$v, PDO::PARAM_STR];
        } elseif ($type === 'int') {
            $alias = self::ALIAS_NUMERIC;
            $suffix = self::SUFFIX_NUMBER;
            $unCastFn = static fn(int|null $v) => $v + 0;
            $castFn = static fn(int $v) => [$v, PDO::PARAM_INT];
        } elseif ($type === 'float') {
            $alias = self::ALIAS_NUMERIC;
            $suffix = self::SUFFIX_NUMBER;
            $unCastFn = static fn(string|float|null $v) => $v + 0;
            $castFn = static fn(float $v) => [$v, PDO::PARAM_STR];
        } elseif ($type === 'bool') {
            $alias = self::ALIAS_BOOLEAN;
            $suffix = self::SUFFIX_BOOLEAN;
            $unCastFn = static fn(int|null $v) => (bool)$v;
            $castFn = static fn(bool $v) => [$v, PDO::PARAM_BOOL];
        } elseif (is_subclass_of($type, DateTimeInterface::class)) {
            $alias = self::ALIAS_DATETIME;
            $suffix = self::SUFFIX_DATETIME;
            $unCastFn = static fn(string|null $v) => $v && ($v !== static::EMPTY_DATE)
                ? $type::createFromFormat('Y-m-d H:i:s', $v)
                : DateUtil::minDate();
            $castFn = static fn(DateTimeInterface $v) => [
                DateUtil::isMinDate($v) ? static::EMPTY_DATE : $v->format('Y-m-d H:i:s'),
                PDO::PARAM_STR
            ];
        } elseif (is_subclass_of($type, BackedEnum::class)) {
            $bakingType = (new \ReflectionEnum($type))->getBackingType()->getName();
            $alias = $this->attributeReader->getAlias($type);
            if ($bakingType === 'int') {
                $suffix = self::SUFFIX_NUMBER;
                $unCastFn = static fn(int|null $v) => $type::tryFrom($v);
                $castFn = static fn(BackedEnum $v) => [$v->value, PDO::PARAM_INT];
            } elseif ($bakingType === 'string') {
                $suffix = self::SUFFIX_STRING;
                $unCastFn = static fn(string|null $v) => $type::tryFrom($v);
                $castFn = static fn(BackedEnum $v) => [$v->value, PDO::PARAM_STR];
            } else {
                throw new UnexpectedValueException("Unexpected backing type " . $bakingType);
            }
        } elseif (is_subclass_of($type, UnitEnum::class)) {
            $alias = $this->attributeReader->getAlias($type);
            $suffix = self::SUFFIX_ENUM;
            $cases = [];
            foreach ($type::cases() as $case) {
                $cases[$case->name] = $case;
            }
            $unCastFn = static fn(string|null $v) => $cases[$v] ?? null;
            $castFn = static fn(UnitEnum $v) => [$v->name, PDO::PARAM_STR];
        } elseif (is_subclass_of($type, JsonSerializable::class)) {
            $alias = $this->attributeReader->getAlias($type);
            $suffix = self::SUFFIX_JSON;
            $unCastFn = static fn(string|null $v) => new $type($v ? json_decode($v, true) : []);
            $castFn = static fn(JsonSerializable $v) => [json_encode($v->jsonSerialize()), PDO::PARAM_STR];
        } elseif ($this->attributeReader->isRecord($type)) {
            $alias = $this->attributeReader->getAlias($type);
            $suffix = self::SUFFIX_REFERENCE;
            $unCastFn = static fn(string|int|null $v) => $rm->getReference($type, $v === self::EMPTY_KEY ? null : $v);
            $castFn = static fn(object $v) => [
                $rm->getReferenceId($v, true) ?? self::EMPTY_KEY,
                PDO::PARAM_STR
            ];
        } else {
            throw new UnexpectedValueException("Unexpected type " . $type);
        }
        $result = [
            'type' => $alias,
            'suffix' => $suffix,
            'castFn' => $castFn,
            'unCastFn' => $unCastFn,
        ];
        $this->typeInfoCache[$type] = $result;
        return $result;
    }

    protected function getCompositeBinding(Field $field, mixed $value): array
    {
        $type = get_debug_type($value);
        $typeInfo = $this->getTypeInfo($type);

        if (count($field->columns) === 1) {
            return [reset($field->columns)->name => $value];
        }

        $fields = [];
        foreach ($field->columns as $column) {
            if (!$column->storedTypes) {
                $fields[$column->name] = $typeInfo['type'];
            } elseif (in_array($type, $column->storedTypes, true)) {
                $fields[$column->name] = $value;
            } else {
                $fields[$column->name] = $column->defaultValue;
            }
        }
        return $fields;
    }

    /**
     * @param PDOStatement $stmt
     * @param Record $metadata
     * @param array<object> $records
     * @param array<string, Field> $fields
     * @param array<int, string> $sqlFields
     * @param int $initialIndex
     * @return void
     */
    protected function bindOrderedPDOParams(
        PDOStatement $stmt,
        Record $metadata,
        array $records,
        array $fields,
        array $sqlFields,
        int $initialIndex = 1,
    ): void {
        foreach ($records as $record) {
            foreach ($fields as $field) {
                if ($metadata->isReference && $metadata->referenceField === $field->name) {
                    $this->bindPDOValue($stmt, $initialIndex + $sqlFields[$metadata->primaryKey->columns[0]], $record);
                } else {
                    $map = $this->getCompositeBinding($field, $record->{$field->name});
                    foreach ($map as $sqlField => $sqlValue) {
                        $this->bindPDOValue($stmt, $initialIndex + $sqlFields[$sqlField], $sqlValue);
                    }
                }
            }
            $initialIndex += count($sqlFields);
        }
    }

    public function getRecordMetadata(string $class): Record
    {
        if (isset($this->metadataCache[$class])) {
            return $this->metadataCache[$class];
        }
        $metadata = $this->attributeReader->getRecordAttribute($class);

        foreach ($metadata->fields as $field) {
            $this->populateFieldData($field);
        }

        foreach ($metadata->indexes as $index) {
            if ($index->columns === null) {
                $index->columns = [];
                foreach ($index->fields as $fieldName) {
                    $columns = array_keys($metadata->fields[$fieldName]->columns);
                    $index->columns = array_merge($index->columns, $columns);
                }
            }
        }

        $this->metadataCache[$class] = $metadata;
        return $metadata;
    }

    protected function getDecoder(string $fieldName, array $types): Closure
    {
        if (count($types) === 1) {
            $typeInfo = $this->getTypeInfo($types[0]);
            $column = $this->getColumnName($fieldName);
            $baseFn = $typeInfo['unCastFn'];
            return static fn(array $dbRow) => $baseFn($dbRow[$column] ?? null);
        }
        $typeColumn = $this->getColumnName($fieldName, self::SUFFIX_TYPE);
        $byType = [];
        foreach ($types as $type) {
            $typeInfo = $this->getTypeInfo($type);
            $column = $this->getColumnName($fieldName, $typeInfo['suffix']);
            $byType[$typeInfo['type']] = [
                $column,
                $typeInfo['unCastFn']
            ];
        }
        return static function (array $dbRow) use ($typeColumn, $byType) {
            [$column, $fn] = $byType[$dbRow[$typeColumn]];
            return $fn($dbRow[$column]);
        };
    }

    public function bindPDOValue(PDOStatement $stmt, int|string $key, mixed $value): bool
    {
        $castFn = $this->getTypeInfo(get_debug_type($value))['castFn'];
        [$v, $t] = $castFn($value);
        return $stmt->bindValue($key, $v, $t);
    }

    public function sync(array $classes, SyncData $event): void
    {
        foreach ($classes as $class) {
            $this->upsertRecords(
                $class,
                array_merge($event->getCreatedRecords($class), $event->getUpdatedRecords($class))
            );
            $this->deleteRecords($class, $event->getDeletedRecords($class));
        }
    }

    protected function upsertRecords(string $class, array $records): void
    {
        if (!$records) {
            return;
        }
        $metadata = $this->getRecordMetadata($class);

        $sqlFields = $onDuplicate = [];
        foreach ($metadata->fields as $field) {
            foreach ($field->columns as $column) {
                $sqlFields[$column->name] = count($sqlFields);
                if (!in_array($column->name, $metadata->primaryKey->columns)) {
                    $onDuplicate[] = "`$column->name` = VALUES(`$column->name`)";
                }
            }
        }
        $paramsPlaceholder = '(' . implode(',', array_fill(0, count($sqlFields), '?')) . ')';

        $chunks = array_chunk($records, $metadata->insertBatchLimit);
        foreach ($chunks as $records) {
            /** @noinspection SqlResolve */
            $pattern = "INSERT INTO `%s`(`%s`) VALUES %s";
            $sql = sprintf(
                $pattern,
                $metadata->table,
                implode('`,`', array_keys($sqlFields)),
                implode(',', array_fill(0, count($records), $paramsPlaceholder))
            );
            $sql .= " ON DUPLICATE KEY UPDATE " . implode(',', $onDuplicate);
            $stmt = $this->connection->prepare($sql);
            $this->bindOrderedPDOParams($stmt, $metadata, $records, $metadata->fields, $sqlFields);
            $stmt->execute();
        }
    }

    protected function deleteRecords(string $class, array $records): void
    {
        if (!$records) {
            return;
        }
        $metadata = $this->getRecordMetadata($class);
        $pkFields = $sqlFields = [];
        foreach ($metadata->fields as $field) {
            if (in_array($field->name, $metadata->primaryKey->fields)) {
                $pkFields[] = $field;
            }
        }
        foreach ($metadata->primaryKey->columns as $column) {
            $sqlFields[$column] = count($sqlFields);
        }
        if (count($sqlFields) > 1) {
            $paramsPlaceholder = '`' . implode('` = ? AND `', array_keys($sqlFields)) . '` = ?';
            $paramsPlaceholder = "($paramsPlaceholder)";
        } else {
            $paramsPlaceholder = implode(',', array_fill(0, count($sqlFields), '?'));
        }

        $chunks = array_chunk($records, $metadata->deleteBatchLimit);
        foreach ($chunks as $records) {
            if (count($sqlFields) > 1) {
                /** @noinspection SqlResolve */
                $pattern = "DELETE FROM `%s` WHERE %s";
                $sql = sprintf(
                    $pattern,
                    $metadata->table,
                    implode(' OR ', array_fill(0, count($records), $paramsPlaceholder)),
                );
            } else {
                /** @noinspection SqlResolve */
                $pattern = "DELETE FROM `%s` WHERE (`%s`) IN (%s)";
                $sql = sprintf(
                    $pattern,
                    $metadata->table,
                    implode('`,`', array_keys($sqlFields)),
                    implode(',', array_fill(0, count($records), $paramsPlaceholder)),
                );
            }
            $stmt = $this->connection->prepare($sql);
            $this->bindOrderedPDOParams($stmt, $metadata, $records, $pkFields, $sqlFields);
            $stmt->execute();
        }
    }

    public function getQuery(string $sql, mixed ...$parameters): TypedQuery
    {
        $paramEncoder = fn(mixed $value) => ($this->getTypeInfo(get_debug_type($value))['castFn'])($value);
        $decoderProvider = fn(string $name, array $phpTypes) => $this->getDecoder($name, $phpTypes);
        $q = new TypedQuery(
            query: $sql,
            connection: $this->connection,
            paramEncoder: $paramEncoder,
            decoderProvider: $decoderProvider,
        );
        if (count($parameters) === 1 && isset($parameters[0]) && is_array($parameters[0])) {
            $parameters = $parameters[0];
        }
        foreach ($parameters as $key => $value) {
            $q->setParam($key, $value);
        }
        return $q;
    }

    public function beginTransaction(): void
    {
        $this->connection->beginTransaction();
    }

    public function commit(): void
    {
        $this->connection->commit();
    }

    public function rollback(): void
    {
        $this->connection->rollback();
    }

    #[ArrayShape(['sql' => 'string', 'params' => 'array'])]
    public function constructQueryFilter(Field $field, string $operation, mixed $item): array
    {
        $params = [];
        if (is_array($item)) {
            if (count($field->columns) !== 1) {
                // can be improved by traversing array elements
                throw new UnexpectedValueException("Unsupported operation $operation for field $field->name");
            }
            if (count($item) === 0) {
                $sql = 'FALSE';
            } else {
                $columnName = reset($field->columns)->name;
                $paramName = uniqid('L');
                if ($operation === '=') {
                    $sql = "`$columnName` IN (:$paramName)";
                } elseif ($operation === '!=') {
                    $sql = "`$columnName` NOT IN (:$paramName)";
                } else {
                    throw new UnexpectedValueException("Unsupported operation $operation for field $field->name");
                }
                $params[$paramName] = $item;
            }
        } else {
            if (count($field->columns) !== 1 && !in_array($operation, ['=', '!='])) {
                throw new UnexpectedValueException("Unsupported operation $operation for field $field->name");
            }
            if (in_array($operation, ['~', '!~', '~~', '!~~']) && $field->phpTypes[0] !== 'string') {
                throw new UnexpectedValueException("Unsupported operation $operation for field $field->name");
            }
            $sqlOp = match ($operation) {
                '=', '<=', '<', '>', '>=' => $operation,
                '!=' => '<>',
                '~', '~~' => 'LIKE',
                '!~', '!~~' => 'NOT LIKE',
                default => throw new UnexpectedValueException(
                    "Unsupported operation $operation for field $field->name"
                )
            };
            $sql = [];
            $map = $this->getCompositeBinding($field, $item);
            foreach ($map as $sqlField => $sqlValue) {
                $paramName = uniqid('F');
                $sql[] = "`$sqlField` $sqlOp :$paramName";
                if (in_array($operation, ['~', '!~'])) {
                    assert(is_string($sqlValue));
                    $sqlValue = str_replace(['%', '_'], ['\%', '\_'], $sqlValue) . '%';
                } elseif (in_array($operation, ['~~', '!~~'])) {
                    assert(is_string($sqlValue));
                    $sqlValue = '%' . str_replace(['%', '_'], ['\%', '\_'], $sqlValue) . '%';
                }
                $params[$paramName] = $sqlValue;
            }
            if (count($sql) > 1) {
                if ($operation === '=') {
                    $sql = '(' . implode(' AND ', $sql) . ')';
                } elseif ($operation === '!=') {
                    $sql = '(' . implode(' OR ', $sql) . ')';
                } else {
                    assert(false);
                }
            } else {
                $sql = $sql[0];
            }
        }
        return [
            'sql' => $sql,
            'params' => $params
        ];
    }

}