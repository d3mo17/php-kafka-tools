<?php

namespace DMo\Kaftan;

/**
 * Helper to handle data from kafka
 */
class AvroData
{
    /**
     * @var AvroSchema
     */
    private $schema;

    /**
     * @param string $schema_json
     */
    function __construct(string $schema_json)
    {
        $this->schema = \AvroSchema::parse($schema_json);
    }

    /**
     * Normalize avro binary data
     *
     * @param string $binary
     * @return string
     */
    private static function normalize(string $binary) : string
    {
        $stx = "\002";
        return strstr($binary, $stx);
    }

    /**
     * Converts binary avro data to structure array
     *
     * @param string $binary
     * @return void
     */
    public function convertBinaryAvro(string $binary) : array
    {
        $reader = new \AvroIODatumReader($this->schema);
        $io = new \AvroStringIO(self::normalize($binary));
        
        return $reader->read(new \AvroIOBinaryDecoder($io));
    }

    /**
     * Converts structure array to avro binary data
     *
     * @param array $datum
     * @return string
     */
    public function toBinaryAvro(array $datum) : string
    {
        $writer = new \AvroIODatumWriter($this->schema);
        $io = new \AvroStringIO();

        $writer->write($datum, new \AvroIOBinaryEncoder($io));

        return $io->string();
    }
}
