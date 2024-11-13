<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Utility;

use ReflectionAttribute;
use ReflectionClass;
use ReflectionNamedType;
use ReflectionUnionType;
use UnexpectedValueException;
use vhd\BlazingOrm\Metadata\Alias;
use vhd\BlazingOrm\Metadata\Field;
use vhd\BlazingOrm\Metadata\Index;
use vhd\BlazingOrm\Metadata\Record;

class AttributeReader
{

    protected array $aliasCache = [];
    protected array $isRecordCache = [];
    protected array $reflectionCache = [];

    protected static ?AttributeReader $instance = null;

    public static function getInstance(): AttributeReader
    {
        if (!self::$instance) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    public function getRecordAttribute(string $recordClass): Record
    {
        $reflection = $this->getReflectionClass($recordClass);
        $metadata = $reflection->getAttributes(Record::class);
        if (!$metadata) {
            throw new UnexpectedValueException("Record '$recordClass' must contain 'Record' attribute");
        }
        /** @var Record $metadata */
        $metadata = $metadata[0]->newInstance();

        $pkIndex = null;
        foreach ($reflection->getProperties() as $property) {
            $fieldAttribute = $property->getAttributes(Field::class);
            if (!$fieldAttribute) {
                continue;
            }
            /** @var Field $fieldAttribute */
            $fieldAttribute = $fieldAttribute[0]->newInstance();
            $fieldAttribute->name = $property->getName();
            $metadata->fields[$fieldAttribute->name] = $fieldAttribute;

            $propertyTypes = $property->getType();
            if ($propertyTypes instanceof ReflectionUnionType) {
                $propertyTypes = $propertyTypes->getTypes();
                usort($propertyTypes, function ($a, $b) {
                    return $b->isBuiltin() <=> $a->isBuiltin();
                });
            } elseif ($propertyTypes instanceof ReflectionNamedType) {
                $propertyTypes = [$propertyTypes];
            } else {
                throw new UnexpectedValueException(
                    'Invalid field definition ' . $property->getDeclaringClass() . '::' . $property
                );
            }
            foreach ($propertyTypes as $type) {
                $typeName = $type->getName();
                if ($typeName === 'self') {
                    $typeName = $property->getDeclaringClass()->getName();
                }
                $fieldAttribute->phpTypes[] = $typeName;
            }
            $fieldAttribute->phpTypes = array_unique($fieldAttribute->phpTypes);

            if ($fieldAttribute->primary) {
                if (!$pkIndex) {
                    $pkIndex = new Index(
                        name: 'PRIMARY',
                        fields: [$fieldAttribute->name],
                        unique: true
                    );
                    $metadata->indexes[] = $pkIndex;
                } else {
                    $pkIndex->fields[] = $fieldAttribute->name;
                }
            }
            if ($fieldAttribute->index) {
                $keyName = 'by_' . $fieldAttribute->name;
                $index = new Index(
                    name: $keyName,
                    fields: [$fieldAttribute->name],
                );
                $metadata->indexes[] = $index;
            }
        }

        foreach ($metadata->indexes as $index) {
            if (!$metadata->primaryKey && ($index->name === 'PRIMARY' || $index->unique)) {
                $metadata->primaryKey = $index;
            }
        }

        if ($metadata->primaryKey
            && count($metadata->primaryKey->fields) === 1
        ) {
            $pkField = $metadata->fields[$metadata->primaryKey->fields[0]];
            if (count($pkField->phpTypes) === 1
                && ($pkField->phpTypes[0] === $recordClass || $pkField->phpTypes[0] === 'self')
            ) {
                $metadata->isReference = true;
                $metadata->referenceField = $pkField->name;
            }
        }

        if (!$metadata->primaryKey) {
            $fieldAttribute = new Field(
                primary: true,
                name: 'id',
                phpTypes: [$recordClass],
            );
            $metadata->fields = [$fieldAttribute->name => $fieldAttribute] + $metadata->fields;
            $pkIndex = new Index(
                name: 'PRIMARY',
                fields: [$fieldAttribute->name],
            );
            array_unshift($metadata->indexes, $pkIndex);
            $metadata->primaryKey = $pkIndex;
            $metadata->isReference = true;
            $metadata->referenceField = $fieldAttribute->name;
        }

        if (!$metadata->isReference) {
            $metadata->fetchBatchLimit = 200;
            $metadata->deleteBatchLimit = 200;
        } else {
            $metadata->fetchBatchLimit = 5000;
            $metadata->deleteBatchLimit = 5000;
        }
        $metadata->insertBatchLimit = 5000;

        return $metadata;
    }

    public function hasCustomAlias(object|string $object): bool
    {
        $class = is_string($object) ? $object : $object::class;
        $reflection = $this->getReflectionClass($class);
        $aliasAttr = $reflection->getAttributes(Alias::class);
        return (bool)$aliasAttr;
    }

    public function getAlias(object|string $object): string
    {
        $class = is_string($object) ? $object : $object::class;
        if (isset($this->aliasCache[$class])) {
            return $this->aliasCache[$class];
        }
        $reflection = $this->getReflectionClass($class);
        $aliasAttr = $reflection->getAttributes(Alias::class);
        if ($aliasAttr) {
            /** @var Alias $alias */
            $alias = $aliasAttr[0]->newInstance();
            $result = $alias->name;
        } else {
            $result = $reflection->getShortName();
        }
        $this->aliasCache[$class] = $result;
        return $result;
    }

    public function isRecord(object|string $record): bool
    {
        $class = is_string($record) ? $record : $record::class;
        if (isset($this->isRecordCache[$class])) {
            return $this->isRecordCache[$class];
        }
        $reflection = $this->getReflectionClass($class);
        $result = (bool)$reflection->getAttributes(Record::class);
        $this->isRecordCache[$class] = $result;
        return $result;
    }

    /**
     * @template T of object
     * @param class-string $class
     * @param class-string<T> $attribute
     * @return T|null
     */
    public function getAttribute(string $class, string $attribute): ?object
    {
        $reflection = $this->getReflectionClass($class);
        $attributes = $reflection->getAttributes($attribute, ReflectionAttribute::IS_INSTANCEOF);
        if (!$attributes) {
            return null;
        }
        return $attributes[0]->newInstance();
    }

    public function getReflectionClass(string $class): ReflectionClass
    {
        if (isset($this->reflectionCache[$class])) {
            return $this->reflectionCache[$class];
        }
        $reflection = new ReflectionClass($class);
        $this->reflectionCache[$class] = $reflection;
        return $reflection;
    }

}