<?php

declare(strict_types=1);

namespace vhd\BlazingOrm;

use ArrayObject;
use DateTimeInterface;
use InvalidArgumentException;
use JsonSerializable;
use LogicException;
use RuntimeException;
use SplObjectStorage;
use Throwable;
use UnexpectedValueException;
use UnitEnum;
use vhd\BlazingOrm\Metadata\Record;
use vhd\BlazingOrm\Utility\AttributeReader;
use vhd\BlazingOrm\Utility\RecordState;
use WeakMap;

class RecordManager implements RecordManagerInterface
{

    protected const string EMPTY_KEY = "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

    /** @var WeakMap<object, RecordState> */
    protected WeakMap $identityMap;

    /** @var array<class-string, array<string, object>> */
    protected array $keyMap = [];

    /** @var SplObjectStorage<object, RecordState> */
    protected SplObjectStorage $attachedRecords;
    protected bool $inFlushMode = false;
    protected bool $inSyncMode = false;

    /** @var array<class-string, StorageEngineInterface> */
    protected array $storageEngines = [];
    /** @var array<class-string, RepositoryInterface> */
    protected array $repositories = [];
    /** @var array<EventListenerInterface> */
    protected array $listeners = [];

    public function __construct(
        protected ?AttributeReader $attributeReader = null,
    ) {
        $this->identityMap = new WeakMap();
        $this->attachedRecords = new SplObjectStorage();
        $this->attributeReader ??= AttributeReader::getInstance();
    }

    /**
     * @template TRecord of object
     * @param class-string<TRecord> $class
     * @param string|array|null $key
     * @return TRecord
     */
    public function getReference(string $class, string|array|null $key): object
    {
        $defaultTask = RecordState::TASK_FETCH;
        if ($this->isReferenceRecord($class)) {
            if (is_string($key)) {
                $len = strlen($key);
                if ($len === 36) {
                    $key = hex2bin(str_replace('-', '', $key));
                    if ($key === false) {
                        throw new InvalidArgumentException('Invalid uuid provided');
                    }
                    $len = strlen($key);
                }
                if ($len !== 16) {
                    throw new InvalidArgumentException('Invalid uuid provided');
                } elseif ($key === self::EMPTY_KEY) {
                    $key = null;
                }
            } elseif ($key !== null) {
                throw new InvalidArgumentException('Invalid key provided');
            }
            if (!$key) {
                $defaultTask = RecordState::TASK_NONE;
            }
            $keyOrHash = $key;
        } else {
            $normalized = $this->normalizeKey($class, $key);
            $keyOrHash = $this->getKeyHash($normalized);
        }

        $record = $this->findByKey($class, $keyOrHash);
        if ($record) {
            return $record;
        }

        $reflection = $this->attributeReader->getReflectionClass($class);
        $record = $reflection->newInstanceWithoutConstructor();
        if (isset($normalized)) {
            foreach ($normalized as $fName => $fValue) {
                $record->$fName = $fValue;
            }
        }

        $state = new RecordState();
        $state->state = RecordState::STATE_REFERENCE;
        $state->task = $defaultTask;
        $this->setKey($record, $keyOrHash, $state);
        $this->identityMap->offsetSet($record, $state);

        return $record;
    }

    public function isNullReference(object $record): ?bool
    {
        if (!$this->isReferenceRecord($record::class)) {
            return null;
        }
        $state = $this->getRecordState($record);
        return $state->state === RecordState::STATE_REFERENCE && !isset($state->key);
    }

    /**
     * @template TRecord of object
     * @param class-string<TRecord> $class
     * @return TRecord
     */
    public function getNullReference(string $class): object
    {
        if (!$this->isReferenceRecord($class)) {
            throw new InvalidArgumentException("Class $class is not a reference record");
        }
        return $this->getReference($class, null);
    }

    public function getReferenceId(object $record, bool $binary): string|null
    {
        if (!$this->isReferenceRecord($record::class)) {
            throw new InvalidArgumentException('Only referenced tables have storable key');
        }
        $state = $this->getRecordState($record);
        if ($state->state === RecordState::STATE_NEW && !isset($state->key)) {
            throw new InvalidArgumentException("Unable to retrieve reference key. Record is not stored");
        }
        if (!$state->key) {
            return null;
        }
        if ($binary) {
            return $state->key;
        }
        $str = bin2hex($state->key);
        return substr($str, 0, 8) . '-' . substr($str, 8, 4) . '-' . substr($str, 12, 4)
            . '-' . substr($str, 16, 4) . '-' . substr($str, 20);
    }

    public function prefetch(null|object|array|string $include = null): array
    {
        $classFilter = [];
        if ($include !== null) {
            $items = is_array($include) ? $include : [$include];
            foreach ($items as $item) {
                if (is_object($item)) {
                    $state = $this->identityMap[$item] ?? null;
                    if (!$state || !isset($state->key)) {
                        continue;
                    }
                    if ($state->task == RecordState::TASK_FETCH) {
                        $classFilter[$item::class] = true;
                    }
                } elseif (is_string($item)) {
                    $classFilter[$item] = true;
                }
            }
            if (!$classFilter) {
                // nothing to fetch
                return [];
            }
        }
        $byClass = [];
        foreach ($this->identityMap as $record => $state) {
            $class = $record::class;
            if ($classFilter && !($classFilter[$class] ?? false)) {
                continue;
            }
            if ($state->task === RecordState::TASK_FETCH) {
                $byClass[$class][] = $record;
            }
        }
        foreach ($byClass as $class => $records) {
            $repository = $this->getRepository($class);
            $metadata = $this->getRecordMetadata($class);
            $pkFields = $metadata->primaryKey->fields;
            foreach (array_chunk($records, $metadata->fetchBatchLimit) as $chunk) {
                $filter = [];
                if ($metadata->isReference) {
                    $filter[$metadata->referenceField] = $chunk;
                } elseif (count($pkFields) === 1) {
                    $field = reset($pkFields);
                    $filter[$field] = array_column($chunk, $field);
                } else {
                    foreach ($chunk as $record) {
                        $rowFilter = [];
                        foreach ($pkFields as $pkField) {
                            $rowFilter[$pkField] = $record->$pkField;
                        }
                        $filter[] = $rowFilter;
                    }
                    $filter = ['or' => $filter];
                }
                $found = $repository->findAll($filter);
                if (count($found) !== count($chunk)) {
                    foreach ($chunk as $record) {
                        $state = $this->identityMap[$record];
                        if ($state->state === RecordState::STATE_REFERENCE) {
                            $state->task = RecordState::TASK_NONE;
                            $state->state = RecordState::STATE_DELETED;
                        }
                    }
                }
            }
        }
        return $byClass;
    }

    public function getRepository(string $class): RepositoryInterface
    {
        if (!isset($this->repositories[$class])) {
            $metadata = $this->getRecordMetadata($class);
            if ($metadata->repository) {
                $cls = $metadata->repository;
                $repo = new $cls(recordClass: $class, recordManager: $this);
            } else {
                $repo = new Repository(recordClass: $class, recordManager: $this);
            }
            $this->repositories[$class] = $repo;
        }
        return $this->repositories[$class];
    }

    public function findOne(string $class, array $filter = [], array $order = []): ?object
    {
        return $this->getRepository($class)->findOne($filter, $order);
    }

    public function findAll(string $class, array $filter = [], array $order = [], ?int $limit = null): array
    {
        return $this->getRepository($class)->findAll($filter, $order, $limit);
    }

    public function isNew(object $record): ?bool
    {
        $state = $this->identityMap[$record] ?? null;
        if (!$state) {
            return null;
        }
        return $state->state === RecordState::STATE_NEW;
    }

    public function isDeleted(object $record): ?bool
    {
        $state = $this->identityMap[$record] ?? null;
        if (!$state) {
            return null;
        }
        return $state->state === RecordState::STATE_DELETED;
    }

    public function isStored(object $record): ?bool
    {
        $state = $this->identityMap[$record] ?? null;
        if (!$state) {
            return null;
        }
        return $state->state === RecordState::STATE_STORED;
    }

    public function isManaged(object $record): bool
    {
        return $this->identityMap->offsetExists($record);
    }

    public function make(string $class, array $data = []): object
    {
        if ($data) {
            $metadata = $this->getRecordMetadata($class);
            $pkFields = $metadata->primaryKey->fields;
            $dataPKFields = array_intersect($pkFields, array_keys($data));
        }

        if ($data && count($pkFields) === count($dataPKFields)) {
            // data contains direct reference, usually from repository
            if ($metadata->isReference) {
                $record = $data[$metadata->referenceField];
            } else {
                $record = $this->getReference($class, $data);
            }
            $state = $this->getRecordState($record);

            if ($state->state === RecordState::STATE_REFERENCE) {
                $updateRecord = true;
            } elseif ($state->state === RecordState::STATE_STORED) {
                // currently always false, since there is no way to forcefully re-fetch record
                $updateRecord = ($state->task == RecordState::TASK_FETCH);
            } else {
                throw new UnexpectedValueException('Invalid record state');
            }
        } else {
            if ($data) {
                $reflection ??= $this->attributeReader->getReflectionClass($class);
                $record = $reflection->newInstanceWithoutConstructor();
            } else {
                $record = new $class();
            }
            $updateRecord = true;
        }

        if ($updateRecord) {
            $metadata ??= $this->getRecordMetadata($class);
            $reflection ??= $this->attributeReader->getReflectionClass($class);
            $allDataPresent = true;
            foreach ($metadata->fields as $field) {
                if ($metadata->isReference && $metadata->referenceField === $field->name) {
                    continue;
                }
                $property = $reflection->getProperty($field->name);
                if (array_key_exists($field->name, $data)) {
                    $property->setValue($record, $data[$field->name]);
                    continue;
                }
                $allDataPresent = false;
                if ($property->isInitialized($record)) {
                    continue;
                }
                foreach ($field->phpTypes as $type) {
                    if (class_exists($type)) {
                        if ($this->attributeReader->isRecord($type)
                            && $this->isReferenceRecord($type)
                        ) {
                            $property->setValue($record, $this->getNullReference($type));
                            break;
                        } elseif (is_subclass_of($type, DateTimeInterface::class)) {
                            $property->setValue($record, new $type());
                            break;
                        } elseif (is_subclass_of($type, UnitEnum::class)) {
                            $property->setValue($record, $type::cases()[0]);
                            break;
                        } elseif (is_subclass_of($type, JsonSerializable::class)) {
                            $property->setValue($record, new $type());
                            break;
                        }
                    } else {
                        $value = match ($type) {
                            'null' => null,
                            'string' => '',
                            'int' => 0,
                            'float' => 0.0,
                            'bool' => false,
                            default => $property,
                        };
                        if ($value !== $property) {
                            $property->setValue($record, $value);
                            break;
                        }
                    }
                }
            }
            if (isset($state)) {
                assert($allDataPresent); // should never happen
                $state->state = RecordState::STATE_STORED;
                if ($state->task == RecordState::TASK_FETCH) {
                    $state->task = RecordState::TASK_NONE;
                }
            }
        }

        return $record;
    }

    public function persist(object $record): static
    {
        $this->assertNotInSyncMode();

        if (!$this->identityMap->offsetExists($record)) {
            $state = new RecordState();
            $state->state = RecordState::STATE_NEW;
            $state->task = RecordState::TASK_STORE;

            if (!$this->isReferenceRecord($record::class)) {
                $normalized = $this->normalizeKey($record::class, $record);
                $key = $this->getKeyHash($normalized);
                $existing = $this->findByKey($record::class, $key);
                if ($existing) {
                    $existingState = $this->getRecordState($existing);
                    if ($existingState->state !== RecordState::STATE_DELETED) {
                        throw new InvalidArgumentException(
                            'Record with the same key already exists ' . $record::class . ":" . $key
                        );
                    }
                }
                $this->setKey($record, $key, $state);
            }
            $this->identityMap->offsetSet($record, $state);
        } else {
            $state = $this->identityMap->offsetGet($record);
        }

        if (!$state->canBePersisted()) {
            throw new InvalidArgumentException('Record cannot be persisted because it is not in a valid state');
        }

        $state->task = RecordState::TASK_STORE;
        $this->attachedRecords->attach($record);
        return $this;
    }

    public function remove(object $record): static
    {
        $this->assertNotInSyncMode();

        $state = $this->getRecordState($record);
        if (!$state->canBeRemoved()) {
            throw new InvalidArgumentException('Record cannot be removed because it is not persisted');
        }
        $state->task = RecordState::TASK_DELETE;
        $this->attachedRecords->attach($record);
        return $this;
    }

    public function flush(): void
    {
        if ($this->inFlushMode) {
            throw new LogicException('Recursive flush call detected');
        }
        $this->inFlushMode = true;

        try {
            foreach ($this->listeners as $listener) {
                [$created, $updated, $deleted] = $this->collectSyncData();
                $syncData = new SyncData($created, $updated, $deleted);
                $listener->beforeFlush($this, $syncData);
            }
        } catch (Throwable $e) {
            $this->inFlushMode = false;
            throw $e;
        }

        $this->assertNotInSyncMode();
        $this->inSyncMode = true;

        $syncData = $this->sync();

        $this->attachedRecords = new SplObjectStorage();
        $this->inSyncMode = false;

        try {
            foreach ($this->listeners as $listener) {
                $listener->afterFlush($this, $syncData);
            }
        } catch (Throwable $e) {
            $this->inFlushMode = false;
            throw $e;
        }

        $this->inFlushMode = false;
    }

    public function detach(object $record): static
    {
        $this->assertNotInSyncMode();
        if (!$this->attachedRecords->contains($record)) {
            throw new InvalidArgumentException('Record is not attached');
        }
        $state = $this->getRecordState($record);
        assert($state->task === RecordState::TASK_STORE || $state->task === RecordState::TASK_DELETE);
        $this->attachedRecords->detach($record);
        $state->task = RecordState::TASK_NONE;
        return $this;
    }

    public function clear(): void
    {
        $this->assertNotInSyncMode();
        $this->attachedRecords = new SplObjectStorage();
    }

    public function free(?array $classes = null): void
    {
        if ($classes !== null) {
            foreach ($classes as $class) {
                unset($this->keyMap[$class]);
            }
        } else {
            $this->keyMap = [];
        }
        gc_collect_cycles();
        foreach ($this->identityMap as $record => $state) {
            if (isset($state->key) || $state->state === RecordState::STATE_REFERENCE) {
                $this->keyMap[$record::class][$state->key] = $record;
            }
        }
    }

    public function getStorageEngine(string $class): StorageEngineInterface
    {
        return $this->storageEngines[$class] ??= $this->storageEngines['*'];
    }

    public function addStorageEngine(StorageEngineInterface $engine, string $class = '*'): void
    {
        if (isset($this->storageEngines[$class])) {
            throw new InvalidArgumentException('Storage engine for ' . $class . ' already set');
        }
        $this->storageEngines[$class] = $engine;
    }

    public function beginTransaction(array $classes): void
    {
        $engines = array_map(fn($class) => $this->getStorageEngine($class), $classes);
        $engines = array_unique($engines, SORT_REGULAR);
        array_walk($engines, fn(StorageEngineInterface $engine) => $engine->beginTransaction());
    }

    public function commit(array $classes): void
    {
        $engines = array_map(fn($class) => $this->getStorageEngine($class), $classes);
        $engines = array_unique($engines, SORT_REGULAR);
        array_walk($engines, fn(StorageEngineInterface $engine) => $engine->commit());
    }

    public function rollback(array $classes): void
    {
        $engines = array_map(fn($class) => $this->getStorageEngine($class), $classes);
        $engines = array_unique($engines, SORT_REGULAR);
        array_walk($engines, fn(StorageEngineInterface $engine) => $engine->rollback());
    }

    public function pushListener(EventListenerInterface $listener): void
    {
        array_unshift($this->listeners, $listener);
    }

    public function popListener(): EventListenerInterface
    {
        if (!$this->listeners) {
            throw new LogicException('Attempt to pop from an empty event listener stack');
        }
        return array_shift($this->listeners);
    }

    protected function assertNotInSyncMode(): void
    {
        if ($this->inSyncMode) {
            throw new RuntimeException('Unable to modify records state while flush is in progress');
        }
    }

    protected function isReferenceRecord(string $class): bool
    {
        return $this->getRecordMetadata($class)->isReference;
    }

    public function getRecordMetadata(string $class): Record
    {
        return $this->getStorageEngine($class)->getRecordMetadata($class);
    }

    protected function normalizeKey(string $class, mixed $source): array
    {
        $metadata = $this->getRecordMetadata($class);
        if (is_array($source)) {
            $extractor = static fn($key) => $source[$key] ?? null;
        } elseif (is_object($source)) {
            $extractor = static fn($key) => $source->$key ?? null;
        } elseif (count($metadata->primaryKey->fields) === 1) {
            $extractor = static fn($key) => $source;
        } else {
            throw new InvalidArgumentException();
        }
        $pkData = [];
        foreach ($metadata->primaryKey->fields as $fieldName) {
            $pkData[$fieldName] = $extractor($fieldName);
        }
        return $pkData;
    }

    protected function getKeyHash(array $source): string
    {
        $key = [];
        foreach ($source as $value) {
            if (is_object($value)) {
                if ($value instanceof DateTimeInterface) {
                    $value = 'DT@' . $value->format(DateTimeInterface::ATOM);
                } else {
                    $value = $value::class . '@' . spl_object_id($value);
                }
            } else {
                $value = gettype($value) . '@' . $value;
            }
            $key[] = $value;
        }
        return implode('-', $key);
    }

    protected function findByKey(string $class, string|null $keyHash): ?object
    {
        return $this->keyMap[$class][$keyHash] ?? null;
    }

    protected function getRecordState(object $record): RecordState
    {
        if (!$this->identityMap->offsetExists($record)) {
            throw new UnexpectedValueException('Unmanaged record ' . $record::class);
        }
        return $this->identityMap->offsetGet($record);
    }

    protected function setKey(object $record, ?string $key, RecordState $state): void
    {
        $this->keyMap[$record::class][$key] = $record;
        $state->key = $key;
    }

    protected function collectSyncData(): array
    {
        $states = $engines = $metadata = [];
        $created = $updated = $deleted = [];
        foreach ($this->attachedRecords as $record) {
            $class = $record::class;
            $state = $this->identityMap->offsetGet($record);
            $recordEngine = $engines[$class] ??= $this->getStorageEngine($class);
            $recordMetadata = $metadata[$class] ??= $recordEngine->getRecordMetadata($class);
            if ($state->task === RecordState::TASK_STORE) {
                if (!isset($state->key)) {
                    assert($state->state === RecordState::STATE_NEW);
                    assert($recordMetadata->isReference);
                    $this->setKey($record, $this->generateKey(), $state);
                    $created[$class][] = $record;
                } else {
                    if ($state->state === RecordState::STATE_NEW) {
                        $created[$class][] = $record;
                    } else {
                        $updated[$class][] = $record;
                        if (!$recordMetadata->isReference) {
                            $currentKey = $this->getKeyHash($this->normalizeKey($class, $record));
                            assert($state->key === $currentKey);
                        }
                    }
                }
            } elseif ($state->task === RecordState::TASK_DELETE) {
                assert(isset($state->key));
                if (!$recordMetadata->isReference) {
                    $currentKey = $this->getKeyHash($this->normalizeKey($class, $record));
                    assert($state->key === $currentKey);
                }
                $deleted[$class][] = $record;
            } else {
                continue;
            }
            $states[] = $state;
        }
        return [$created, $updated, $deleted, $engines, $states];
    }

    protected function generateKey(): string
    {
        $time = microtime();
        $time = substr($time, 11) . substr($time, 2, 7);
        $time = str_pad(dechex((int)$time + 0x01b21dd213814000), 16, '0', STR_PAD_LEFT);
        $clockSeq = random_int(0, 0x3fff);
        static $node;
        if (null === $node) {
            $node = sprintf('%06x%06x', random_int(0, 0xffffff) | 0x010000, random_int(0, 0xffffff));
        }
        $uuidV1 = sprintf(
            '%08s%04s1%03s%04x%012s',
            substr($time, -8),
            substr($time, -12, 4),
            substr($time, -15, 3),
            $clockSeq | 0x8000,
            $node
        );
        $uuidV6 = substr($uuidV1, 13, 3) . substr($uuidV1, 8, 4) . $uuidV1[0]
            . substr($uuidV1, 1, 4) . '6'
            . substr($uuidV1, 5, 3) . substr($uuidV1, 16);
        return hex2bin($uuidV6);
    }

    protected function sync(): SyncData
    {
        [$created, $updated, $deleted, $engines, $states] = $this->collectSyncData();

        $syncData = new SyncData($created, $updated, $deleted);
        $classesByEngine = new SplObjectStorage();
        foreach ($engines as $class => $engine) {
            $classesByEngine[$engine] ??= new ArrayObject();
            $classesByEngine[$engine]->append($class);
        }

        /** @var StorageEngineInterface[] $uniqEngines */
        $uniqEngines = array_unique($engines, SORT_REGULAR);

        foreach ($uniqEngines as $engine) {
            $engine->beginTransaction();
        }
        foreach ($uniqEngines as $engine) {
            $engine->sync($classesByEngine[$engine]->getArrayCopy(), $syncData);
        }
        foreach ($this->listeners as $listener) {
            $listener->onFlush($this, $syncData);
        }
        foreach ($uniqEngines as $engine) {
            $engine->commit();
        }

        foreach ($states as $state) {
            if ($state->task === RecordState::TASK_STORE) {
                $state->state = RecordState::STATE_STORED;
            } elseif ($state->task === RecordState::TASK_DELETE) {
                $state->state = RecordState::STATE_DELETED;
            }
            $state->task = RecordState::TASK_NONE;
        }

        return $syncData;
    }

}