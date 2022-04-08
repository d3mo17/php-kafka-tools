<?php

namespace DMo\Kaftan;

use Buzz\Client\Curl;
use Jobcloud\Kafka\SchemaRegistryClient\ErrorHandler;
use Jobcloud\Kafka\SchemaRegistryClient\HttpClient;
use Jobcloud\Kafka\SchemaRegistryClient\KafkaSchemaRegistryApiClient;
use Nyholm\Psr7\Factory\Psr17Factory;

/**
 * Client to communicate with REST-API of schema registry
 */
class SchemaRegistryClient extends KafkaSchemaRegistryApiClient
{
    /**
     * @param string $url
     * @param string|null $username
     * @param string|null $password
     */
    public function __construct(string $url, string $username = null, string $password = null)
    {
        $psr17Factory = new Psr17Factory();
        $client = new Curl($psr17Factory);

        $registryClient = new HttpClient(
            $client,
            $psr17Factory,
            new ErrorHandler(),
            $url,
            $username ?? null,
            $password ?? null
        );

        parent::__construct($registryClient);
    }
}
