<?php

declare(strict_types=1);

namespace vhd\BlazingOrm;

readonly class SyncData
{

    public function __construct(
        protected array $createdRecords,
        protected array $updatedRecords,
        protected array $deletedRecords,
    ) {
    }

    /**
     * @return array<class-string>
     */
    public function getAffectedClasses(): array
    {
        $allClasses = array_merge(
            array_keys($this->createdRecords),
            array_keys($this->updatedRecords),
            array_keys($this->deletedRecords),
        );
        return array_values(array_unique($allClasses));
    }

    /**
     * @template T of object
     * @param class-string<T> $class
     * @return array<T>
     */
    public function getCreatedRecords(string $class): array
    {
        return $this->createdRecords[$class] ?? [];
    }

    /**
     * @template T of object
     * @param class-string<T> $class
     * @return array<T>
     */
    public function getUpdatedRecords(string $class): array
    {
        return $this->updatedRecords[$class] ?? [];
    }

    /**
     * @template T of object
     * @param class-string<T> $class
     * @return array<T>
     */
    public function getDeletedRecords(string $class): array
    {
        return $this->deletedRecords[$class] ?? [];
    }

    /**
     * @template T of object
     * @param class-string<T> $class
     * @return array<T>
     */
    public function getPersistedRecords(string $class): array
    {
        return array_merge(
            $this->getCreatedRecords($class),
            $this->getUpdatedRecords($class),
        );
    }

}