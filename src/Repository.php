<?php

declare(strict_types=1);

namespace vhd\BlazingOrm;

use ArrayObject;
use Closure;
use Exception;
use InvalidArgumentException;
use JetBrains\PhpStorm\ArrayShape;
use SplObjectStorage;
use UnexpectedValueException;
use vhd\BlazingOrm\Engine\AbstractSQLEngine;
use vhd\BlazingOrm\Engine\MySQLStorageEngine;
use vhd\BlazingOrm\Metadata\Field;
use vhd\BlazingOrm\Metadata\Record;

/**
 * @template TRecord of object
 * @implements RepositoryInterface<TRecord>
 */
class Repository implements RepositoryInterface
{

    protected Record $metadata;
    protected StorageEngineInterface $engine;

    public function __construct(
        protected string $recordClass,
        protected RecordManagerInterface $recordManager,
    ) {
        $this->engine = $this->recordManager->getStorageEngine($this->recordClass);
        $this->metadata = $this->engine->getRecordMetadata($this->recordClass);
    }

    /** @return TRecord|null */
    public function findOne(array $filter = [], array $order = []): ?object
    {
        $query = $this->createQuery($filter, $order, ['limit' => 1]);
        $result = $this->runQuery($query);
        return $result['records'][0] ?? null;
    }

    /** @return array<TRecord> */
    public function findAll(array $filter = [], array $order = [], ?int $limit = null): array
    {
        $query = $this->createQuery($filter, $order, $limit ? ['limit' => $limit] : []);
        $result = $this->runQuery($query);
        return $result['records'];
    }

    protected function fetchRelation(
        array $records,
        string $property,
        string $targetClass,
        string $targetField,
        null|string|Closure $orderBy = null,
    ): array {
        $rm = $this->recordManager;
        $records = array_filter(
            $records,
            static fn($record) => !isset($record->$property) && !$rm->isNullReference($record)
        );
        if (!$records) {
            return [];
        }

        if (is_string($orderBy)) {
            $orderBy = static fn($a, $b) => $a->$orderBy <=> $b->$orderBy;
        }

        $relationsByRecord = new SplObjectStorage();

        $targetMetadata = $this->recordManager->getRecordMetadata($targetClass);
        $chunks = array_chunk($records, $targetMetadata->fetchBatchLimit);
        $relations = [];
        foreach ($chunks as $chunk) {
            $fetched = $this->recordManager->getRepository($targetClass)->findAll([$targetField => $chunk]);
            foreach ($fetched as $relation) {
                $relationsByRecord[$relation->$targetField] ??= new ArrayObject();
                $relationsByRecord[$relation->$targetField]->append($relation);
                $relations[] = $relation;
            }
        }

        foreach ($records as $record) {
            if ($relationsByRecord->offsetExists($record)) {
                $list = $relationsByRecord[$record]->getArrayCopy();
                if ($orderBy) {
                    usort($list, $orderBy);
                }
                $record->$property = $list;
            } else {
                $record->$property = [];
            }
        }
        return $relations;
    }

    public function createQuery(array $filter = [], array $order = [], array $options = []): array
    {
        $query = ['filter' => [], 'order' => [], 'options' => $options];
        if ($filter) {
            $this->applyQueryFilter($query, 'and', $filter);
        }
        foreach ($order as $key => $item) {
            $this->applyQueryOrder($query, $key, $item);
        }
        return $query;
    }

    #[ArrayShape(['records' => 'array<T>', 'total' => '?int'])]
    /** @return array{records: array<TRecord>, total: ?int} */
    public function runQuery(mixed &$query): array
    {
        $total = null;
        $options = $query['options'] ?? [];
        $limit = (int)($options['limit'] ?? 0);
        $offset = (int)($options['offset'] ?? 0);
        $calculateTotal = (bool)($options['calculateTotal'] ?? false);

        if ($this->engine instanceof AbstractSQLEngine) {
            $params = [];
            $buildWhere = static function (array $filter, string $glue = '') use (&$params, &$buildWhere): string {
                $sql = [];
                foreach ($filter as $filterItem) {
                    if ($filterItem['sql'] ?? null) {
                        $sql[] = $filterItem['sql'];
                        $params = $params + $filterItem['params'];
                        continue;
                    }
                    foreach ($filterItem as $k => $v) {
                        assert(in_array($k, ['and', 'or']));
                        $k = strtoupper($k);
                        $sql[] = '(' . $buildWhere($v, " $k ") . ')';
                    }
                }
                return implode($glue, $sql);
            };
            $where = $buildWhere($query['filter']);
            $orderBy = [];
            foreach ($query['order'] as $orderItem) {
                /** @var Field $field */
                [$field, $direction] = $orderItem;
                foreach ($field->columns as $column) {
                    $orderBy[] = "`$column->name` $direction";
                }
            }

            $useMySqlCalcFoundRows = $calculateTotal && $this->engine instanceof MySQLStorageEngine && !$orderBy;
            if ($useMySqlCalcFoundRows) {
                /** @noinspection SqlResolve */
                $sql = "SELECT SQL_CALC_FOUND_ROWS * FROM `{$this->metadata->table}`";
            } else {
                /** @noinspection SqlResolve */
                $sql = "SELECT * FROM `{$this->metadata->table}`";
            }
            if ($where) {
                $sql .= "\nWHERE $where";
            }
            if ($orderBy) {
                $sql .= "\nORDER BY " . implode(', ', $orderBy);
            }
            if ($limit > 0) {
                $sql .= $offset > 0 ? "\nLIMIT $offset, $limit" : "\nLIMIT $limit";
            }
            $nativeQuery = $this->engine
                ->getQuery($sql, ...$params)
                ->setFieldTypes($this->metadata->fields);

            $records = [];
            foreach ($nativeQuery->getIterator() as $row) {
                $records[] = $this->recordManager->make($this->recordClass, $row);
            }
            if ($useMySqlCalcFoundRows) {
                $total = (int)$this->engine->getQuery("SELECT FOUND_ROWS()")->fetchCell();
                $query['total'] = $total;
            } elseif ($calculateTotal) {
                $sql = "SELECT COUNT(*) FROM `{$this->metadata->table}`" . ($where ? "\nWHERE $where" : '');
                $total = (int)$this->engine->getQuery($sql, ...$params)->fetchCell();
                $query['total'] = $total;
            }
        } else {
            throw new Exception('Unsupported engine type');
        }
        return [
            'records' => $records,
            'total' => $total,
        ];
    }

    protected function constructQueryFilter(mixed &$query, Field|string $field, string $operation, mixed $item): void
    {
        if ($field instanceof Field) {
            $result = $this->engine->constructQueryFilter($field, $operation, $item);
            $query['filter'][] = $result;
        } else {
            throw new UnexpectedValueException("Unsupported query filter $field ($operation)");
        }
    }

    protected function applyQueryFilter(mixed &$query, string $key, mixed $item): void
    {
        if ($key === 'or' || $key === 'and') {
            if (!is_array($item)) {
                throw new UnexpectedValueException("Unsupported filter value for '$key' operation");
            }
            $existingFilter = $query['filter'];
            $query['filter'] = [];
            foreach ($item as $k => $v) {
                if (is_int($k)) {
                    $this->applyQueryFilter($query, 'and', $v);
                } else {
                    $this->applyQueryFilter($query, $k, $v);
                }
            }
            if (count($query['filter']) === 1 && isset($query['filter'][0])) {
                $existingFilter[] = $query['filter'][0];
            } else {
                $existingFilter[] = [$key => $query['filter']];
            }
            $query['filter'] = $existingFilter;
        } else {
            $components = explode(' ', $key);
            $fieldName = $components[0];
            $field = $this->metadata->fields[$fieldName] ?? $fieldName;
            $operation = $components[1] ?? '=';
            $this->constructQueryFilter($query, $field, $operation, $item);
        }
    }

    protected function applyQueryOrder(mixed &$query, string $key, mixed $item): void
    {
        $field = $this->metadata->fields[$key] ?? null;
        if (!$field) {
            throw new InvalidArgumentException("Field $key not found");
        }
        if ($item === 'ASC' || $item === '+') {
            $direction = 'ASC';
        } elseif ($item === 'DESC' || $item === '-') {
            $direction = 'DESC';
        } else {
            throw new InvalidArgumentException("Invalid order direction for field $key");
        }
        $query['order'][] = [$field, $direction];
    }

}