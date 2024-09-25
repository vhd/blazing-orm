<?php

declare(strict_types=1);

namespace vhd\BlazingOrm;

use vhd\BlazingOrm\Metadata\Field;
use vhd\BlazingOrm\Metadata\Record;

interface StorageEngineInterface
{

    public function getRecordMetadata(string $class): Record;

    public function sync(array $classes, SyncData $event): void;

    public function constructQueryFilter(Field $field, string $operation, mixed $item): array;

    public function beginTransaction(): void;

    public function commit(): void;

    public function rollback(): void;

}