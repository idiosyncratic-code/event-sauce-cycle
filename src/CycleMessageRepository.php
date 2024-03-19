<?php

declare(strict_types=1);

namespace Idiosyncratic\EventSauce\Cycle;

use Cycle\Database\DatabaseInterface;
use EventSauce\EventSourcing\AggregateRootId;
use EventSauce\EventSourcing\Header;
use EventSauce\EventSourcing\Message;
use EventSauce\EventSourcing\MessageRepository;
use EventSauce\EventSourcing\OffsetCursor;
use EventSauce\EventSourcing\PaginationCursor;
use EventSauce\EventSourcing\Serialization\MessageSerializer;
use EventSauce\EventSourcing\UnableToPersistMessages;
use EventSauce\EventSourcing\UnableToRetrieveMessages;
use EventSauce\IdEncoding\BinaryUuidIdEncoder;
use EventSauce\IdEncoding\IdEncoder;
use EventSauce\MessageRepository\TableSchema\DefaultTableSchema;
use EventSauce\MessageRepository\TableSchema\TableSchema;
use Generator;
use LogicException;
use Ramsey\Uuid\Uuid;
use Throwable;
use Traversable;

use function array_keys;
use function array_map;
use function array_merge;
use function count;
use function implode;
use function json_decode;
use function json_encode;
use function sprintf;

class CycleMessageRepository implements MessageRepository
{
    private TableSchema $tableSchema;

    private IdEncoder $aggregateRootIdEncoder;

    private IdEncoder $eventIdEncoder;

    public function __construct(
        private readonly DatabaseInterface $db,
        private readonly string $tableName,
        private readonly MessageSerializer $serializer,
        private readonly int $jsonEncodeOptions = 0,
        TableSchema|null $tableSchema = null,
        IdEncoder|null $aggregateRootIdEncoder = null,
        IdEncoder|null $eventIdEncoder = null,
    ) {
        $this->tableSchema = $tableSchema ?? new DefaultTableSchema();
        $this->aggregateRootIdEncoder = $aggregateRootIdEncoder ?? new BinaryUuidIdEncoder();
        $this->eventIdEncoder = $eventIdEncoder ?? $this->aggregateRootIdEncoder;
    }

    public function persist(
        Message ...$messages,
    ) : void {
        if (count($messages) === 0) {
            return;
        }

        $insertColumns = [
            $this->tableSchema->eventIdColumn(),
            $this->tableSchema->aggregateRootIdColumn(),
            $this->tableSchema->versionColumn(),
            $this->tableSchema->payloadColumn(),
            ...array_keys($additionalColumns = $this->tableSchema->additionalColumns()),
        ];

        $insertValues = [];
        $insertParameters = [];

        foreach ($messages as $index => $message) {
            $payload = $this->serializer->serializeMessage($message);
            $payload['headers'][Header::EVENT_ID] ??= Uuid::uuid4()->toString();

            $messageParameters = [
                $this->indexParameter('event_id', $index) => $this->eventIdEncoder->encodeId($payload['headers'][Header::EVENT_ID]),
                $this->indexParameter('aggregate_root_id', $index) => $this->aggregateRootIdEncoder->encodeId($message->aggregateRootId()),
                $this->indexParameter('version', $index) => $payload['headers'][Header::AGGREGATE_ROOT_VERSION] ?? 0,
                $this->indexParameter('payload', $index) => json_encode($payload, $this->jsonEncodeOptions),
            ];

            foreach ($additionalColumns as $column => $header) {
                $messageParameters[$this->indexParameter($column, $index)] = $payload['headers'][$header];
            }

            // Creates a values line like: (:event_id_1, :aggregate_root_id_1, ...)
            $insertValues[] = implode(', ', $this->formatNamedParameters(array_keys($messageParameters)));

            // Flatten the message parameters into the query parameters
            $insertParameters = array_merge($insertParameters, $messageParameters);
        }

        $insertQuery = sprintf(
            "INSERT INTO %s (%s) VALUES\n(%s)",
            $this->tableName,
            implode(', ', $insertColumns),
            implode("),\n(", $insertValues),
        );

        try {
            $this->db->execute($insertQuery, $insertParameters);
        } catch (Throwable $exception) {
            throw UnableToPersistMessages::dueTo('', $exception);
        }
    }

    public function retrieveAll(
        AggregateRootId $id,
    ) : Generator {
        $query = sprintf(
            'SELECT %s FROM %s WHERE %s = :root_id ORDER BY %s ASC',
            $this->tableSchema->payloadColumn(),
            $this->tableName,
            $this->tableSchema->aggregateRootIdColumn(),
            $this->tableSchema->versionColumn(),
        );

        try {
            $result = $this->db->execute(
                $query,
                [
                    'root_id' => $this->aggregateRootIdEncoder->encodeId($id),
                ],
            );

            return $this->yieldMessagesFromPayloads($results);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMessages::dueTo('', $exception);
        }
    }

    /** @psalm-return Generator<Message> */
    public function retrieveAllAfterVersion(
        AggregateRootId $id,
        int $aggregateRootVersion,
    ) : Generator {
        $query = sprintf(
            'SELECT %s FROM %s order by %s ASC',
            $this->tableSchema->payloadColumn(),
            $this->tableName,
            $this->tableSchema->versionColumn(),
        );

        $query = sprintf(
            'SELECT %s FROM %s WHERE %s = :root_id AND WHERE %s > :version ORDER BY %s ASC',
            $this->tableSchema->payloadColumn(),
            $this->tableName,
            $this->tableSchema->aggregateRootIdColumn(),
            $this->tableSchema->versionColumn(),
            $this->tableSchema->versionColumn(),
        );

        $result = $this->db->execute(
            $query,
            ['id' => $incrementalIdColumn],
        );

        try {
            $result = $this->db->execute(
                $query,
                [
                    'root_id' => $this->aggregateRootIdEncoder->encodeId($id),
                    'version' => $aggregateRootVersion,
                ],
            );

            return $this->yieldMessagesFromPayloads($result);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMessages::dueTo('', $exception);
        }
    }

    public function paginate(
        PaginationCursor $cursor,
    ) : Generator {
        if (! $cursor instanceof OffsetCursor) {
            throw new LogicException(
                sprintf(
                    'Wrong cursor type used, expected %s, received %s',
                    OffsetCursor::class,
                    $cursor::class,
                ),
            );
        }

        $numberOfMessages = 0;

        $incrementalIdColumn = $this->tableSchema->incrementalIdColumn();

        $query = sprintf(
            'SELECT %s FROM %s WHERE %s > :id order by %s ASC limit %s',
            $this->tableSchema->payloadColumn(),
            $this->tableName,
            $incrementalColumnId,
            $incrementalColumnId,
            $cursor->limit(),
        );

        try {
            $result = $this->db->execute(
                $query,
                ['id' => $incrementalIdColumn],
            );

            foreach ($result as $payload) {
                $numberOfMessages++;

                yield $this->serializer->unserializePayload(json_decode($payload['payload'], true));
            }
        } catch (Throwable $exception) {
            throw UnableToRetrieveMessages::dueTo($exception->getMessage(), $exception);
        }

        return $cursor->plusOffset($numberOfMessages);
    }

    private function indexParameter(
        string $name,
        int $index,
    ) : string {
        return $name . '_' . $index;
    }

    /**
     * @param array<mixed> $parameters
     *
     * @return array<mixed>
     */
    private function formatNamedParameters(
        array $parameters,
    ) : array {
        return array_map(static fn (string $name) => ':' . $name, $parameters);
    }

    /**
     * @param Traversable<mixed> $payloads
     *
     * @psalm-return Generator<Message>
     */
    private function yieldMessagesFromPayloads(
        Traversable $payloads,
    ) : Generator {
        foreach ($payloads as $payload) {
            yield $message = $this->serializer->unserializePayload(json_decode($payload['payload'], true));
        }

        return isset($message)
                ? $message->header(Header::AGGREGATE_ROOT_VERSION) ?
                : 0
            : 0;
    }
}
