<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Engine;

use PDO;
use PDOStatement;

class PDOWrapper
{

    protected PDO $pdo;
    protected int $transactionLevel = 0;

    public function __construct(
        protected ?string $dsn = null,
        protected ?string $username = null,
        protected ?string $password = null,
        protected ?array $options = null,
        ?PDO $pdo = null,
    ) {
        if ($pdo instanceof PDO) {
            $this->pdo = $pdo;
            return;
        }
        if ($this->options === null) {
            $this->options = [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION];
        }
    }

    public function getPDO(): PDO
    {
        if (!isset($this->pdo)) {
            $this->pdo = new PDO($this->dsn, $this->username, $this->password, $this->options);
        }
        return $this->pdo;
    }

    public function prepare(string $query): PDOStatement
    {
        return $this->getPDO()->prepare($query);
    }

    function beginTransaction(): void
    {
        if (++$this->transactionLevel === 1) {
            $this->getPDO()->beginTransaction();
        }
    }

    function commit(): void
    {
        if (--$this->transactionLevel === 0) {
            $this->getPDO()->commit();
        }
    }

    function rollback(): void
    {
        if ($this->transactionLevel > 0) {
            $this->transactionLevel = 0;
            $this->getPDO()->rollBack();
        }
    }

}