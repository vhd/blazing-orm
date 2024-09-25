<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Utility;

class MetadataCollector
{

    protected array $recordClasses;
    protected array $aliasedClasses;

    public function __construct(
        protected array $dirs,
        protected ?AttributeReader $attributeReader = null,
    ) {
        $this->attributeReader ??= AttributeReader::getInstance();
    }

    /**
     * @return array<class-string>
     */
    public function getRecordClasses(): array
    {
        if (isset($this->recordClasses)) {
            return $this->recordClasses;
        }
        $this->recordClasses = [];
        foreach ($this->dirs as $dir) {
            $files = self::getFilesRecursive($dir, 'php');
            foreach ($files as $file) {
                require_once $file;
            }
        }
        foreach (get_declared_classes() as $class) {
            if ($this->attributeReader->isRecord($class)) {
                $this->recordClasses[] = $class;
            }
        }
        return $this->recordClasses;
    }

    /**
     * @return array<class-string, string>
     */
    public function getAliasedClasses(): array
    {
        if (isset($this->aliasedClasses)) {
            return $this->aliasedClasses;
        }
        $this->aliasedClasses = [];
        foreach ($this->dirs as $dir) {
            $files = self::getFilesRecursive($dir, 'php');
            foreach ($files as $file) {
                require_once $file;
            }
        }
        foreach (get_declared_classes() as $class) {
            if ($this->attributeReader->hasCustomAlias($class)) {
                $this->aliasedClasses[$class] = $this->attributeReader->getAlias($class);
            }
        }
        return $this->aliasedClasses;
    }

    protected static function getFilesRecursive(string $dir, ?string $ext, array &$results = []): array
    {
        $files = scandir($dir);
        foreach ($files as $value) {
            $filename = realpath($dir . DIRECTORY_SEPARATOR . $value);
            if (is_file($filename)) {
                if ($ext === null || pathinfo($filename, PATHINFO_EXTENSION) === $ext) {
                    $results[] = $filename;
                }
            } elseif (is_dir($filename) && $value != "." && $value != "..") {
                self::getFilesRecursive($filename, $ext, $results);
            }
        }
        return $results;
    }

}