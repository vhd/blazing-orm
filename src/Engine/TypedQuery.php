<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Engine;

use Closure;
use Generator;
use PDO;
use PDOStatement;
use ReflectionFunction;
use ReflectionIntersectionType;
use ReflectionNamedType;
use ReflectionUnionType;
use UnexpectedValueException;
use vhd\BlazingOrm\Metadata\Field;

class TypedQuery
{

    protected array $parameters = [];
    protected array $resultTypeMap = [];
    protected string $logQuery;

    public function __construct(
        protected string $query,
        protected PDOWrapper $connection,
        protected Closure $paramEncoder,
        protected Closure $decoderProvider,
    ) {
        $this->logQuery = $query;
    }

    public function setParam(int|string $key, mixed $value): static
    {
        if (is_array($value)) {
            if (!is_string($key)) {
                throw new UnexpectedValueException('Named argument required for list');
            }
            $this->setParamArray($key, $value);
            return $this;
        }
        $this->parameters[$key] = $value;
        return $this;
    }

    protected function setParamArray(string $key, array $values): void
    {
        $placeholder = [];
        $i = count($this->parameters);
        foreach ($values as $value) {
            if (is_array($value)) {
                throw new UnexpectedValueException('Cannot set nested array parameter');
            }
            $k = 'ap_' . ++$i;
            $placeholder[] = ':' . $k;
            $this->setParam($k, $value);
        }
        $key = trim($key, ': ');
        $placeholder = implode(',', $placeholder);
        $this->query = preg_replace('/:' . $key . '(\W|$)/', $placeholder . '$1', $this->query);
        $this->logQuery = preg_replace('/:' . $key . '(\W|$)/', ":[$key(" . count($values) . ")]\$1", $this->logQuery);
    }

    protected function exec(): PDOStatement
    {
        $stmt = $this->connection->prepare($this->query);
        foreach ($this->parameters as $key => $data) {
            [$value, $type] = ($this->paramEncoder)($data);
            $stmt->bindValue(is_string($key) ? $key : $key + 1, $value, $type);
        }
        $stmt->execute();
        return $stmt;
    }

    public function execute(): int
    {
        $stmt = $this->exec();
        return $stmt->rowCount();
    }

    public function setFieldTypes(array $typeMap): static
    {
        foreach ($typeMap as $field => $types) {
            if ($types instanceof Field) {
                $types = $types->phpTypes;
            } elseif (is_string($types)) {
                $types = [$types];
            }
            $this->resultTypeMap[$field] = ($this->decoderProvider)($field, $types);
        }
        return $this;
    }

    /**
     * @return Generator<int, array<string, mixed>>
     */
    public function getIterator(): Generator
    {
        $stmt = $this->exec();
        $index = 0;
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            if ($this->resultTypeMap) {
                $result = [];
                foreach ($this->resultTypeMap as $field => $fn) {
                    $result[$field] = $fn($row);
                }
                yield $index => $result;
            } else {
                yield $index => $row;
            }
            $index++;
        }
    }

    public function fetchCell(): mixed
    {
        foreach ($this->getIterator() as $row) {
            return reset($row);
        }
        return null;
    }

    public function fetchRow(): ?array
    {
        foreach ($this->getIterator() as $row) {
            return $row;
        }
        return null;
    }

    /**
     * @param Closure $callback
     * @return array<string, Closure>
     */
    protected function parseCallback(Closure $callback): array
    {
        $reflection = new ReflectionFunction($callback);
        $typeMap = [];
        foreach ($reflection->getParameters() as $parameter) {
            $field = $parameter->name;
            if ($field === '_row' || $field === '_idx') {
                $typeMap[$field] = null;
                continue;
            }
            $reflectionType = $parameter->getType();
            if ($reflectionType instanceof ReflectionUnionType) {
                $types = array_map(
                    static fn(ReflectionNamedType $type) => $type->getName(),
                    $reflectionType->getTypes()
                );
            } elseif ($reflectionType instanceof ReflectionNamedType) {
                $types = [$reflectionType->getName()];
            } elseif ($reflectionType instanceof ReflectionIntersectionType) {
                throw new UnexpectedValueException("Intersection type is not supported. Parameter '$field'");
            } else {
                $types = ['mixed'];
            }
            if ($types == ['mixed'] && isset($this->resultTypeMap[$field])) {
                $typeMap[$field] = $this->resultTypeMap[$field];
            } elseif ($types == ['mixed']) {
                $typeMap[$field] = static fn($v) => $v[$field];
            } else {
                $typeMap[$field] = ($this->decoderProvider)($field, $types);
            }
        }
        return $typeMap;
    }

    /**
     * @param Closure $callback
     * @return array
     */
    public function map(Closure $callback): array
    {
        $typeMap = $this->parseCallback($callback);
        $stmt = $this->exec();
        $index = 0;
        $result = [];
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $parameters = [];
            foreach ($typeMap as $field => $fn) {
                $parameters[$field] = match ($field) {
                    '_row' => $row,
                    '_idx' => $index,
                    default => $fn($row),
                };
            }
            $result[] = $callback(...$parameters);
            $index++;
        }
        return $result;
    }

}