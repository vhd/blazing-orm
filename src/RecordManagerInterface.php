<?php

declare(strict_types=1);

namespace vhd\BlazingOrm;

interface RecordManagerInterface
{

    /**
     * @template TRecord of object
     * @param class-string<TRecord> $class
     * @return TRecord
     */
    public function getReference(string $class, string|array|null $key): object;

    public function isNullReference(object $record): ?bool;

    /**
     * @template TRecord of object
     * @param class-string<TRecord> $class
     * @return TRecord
     */
    public function getNullReference(string $class): object;

    public function getReferenceId(object $record, bool $binary): string|null;

    public function prefetch(null|object|array|string $include = null): array;

    /**
     * @template TRecord of object
     * @param class-string<TRecord>|TRecord $class
     * @return RepositoryInterface<TRecord>
     */
    public function getRepository(string $class): RepositoryInterface;

    /**
     * @template TRecord of object
     * @param class-string<TRecord> $class
     * @return TRecord|null
     */
    public function findOne(string $class, array $filter = [], array $order = []): ?object;

    /**
     * @template TRecord of object
     * @param class-string<TRecord> $class
     * @return array<TRecord>
     */
    public function findAll(string $class, array $filter = [], array $order = [], ?int $limit = null): array;

    public function isNew(object $record): ?bool;

    public function isDeleted(object $record): ?bool;

    public function isStored(object $record): ?bool;

    public function isManaged(object $record): bool;

    /**
     * @template TRecord
     * @param class-string<TRecord> $class
     * @return TRecord
     */
    public function make(string $class, array $data = []): object;

    public function persist(object $record): static;

    public function remove(object $record): static;

    public function flush(): void;

    public function detach(object $record): static;

    public function clear(): void;

    public function free(?array $classes = null): void;

    public function getStorageEngine(string $class): StorageEngineInterface;

    public function beginTransaction(array $classes): void;

    public function commit(array $classes): void;

    public function rollback(array $classes): void;

    public function pushListener(EventListenerInterface $listener): void;

    public function popListener(): EventListenerInterface;

}