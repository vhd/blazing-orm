<?php

declare(strict_types=1);

namespace vhd\BlazingOrm;

/**
 * @template TRecord
 */
interface RepositoryInterface
{

    /** @return TRecord|null */
    public function findOne(array $filter = [], array $order = []): ?object;

    /** @return array<TRecord> */
    public function findAll(array $filter = [], array $order = [], ?int $limit = null): array;

}