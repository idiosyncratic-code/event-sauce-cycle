<?php

declare(strict_types=1);

namespace Idiosyncratic\EventSauce\Cycle;

use Cycle\Database\Table;
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
use function count;
use function json_decode;
use function json_encode;
use function sprintf;

final class CycleMessageRepository implements MessageRepository
{
    private readonly TableSchema $tableSchema;

    private readonly IdEncoder $aggregateRootIdEncoder;

    private readonly IdEncoder $eventIdEncoder;

    public function __construct(
        private readonly Table $table,
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

        $additionalColumns = $this->tableSchema->additionalColumns();

        $insertColumns = [
            $this->tableSchema->eventIdColumn(),
            $this->tableSchema->aggregateRootIdColumn(),
            $this->tableSchema->versionColumn(),
            $this->tableSchema->payloadColumn(),
            ...array_keys($additionalColumns),
        ];

        $messageRowset = [];

        foreach ($messages as $message) {
            $payload = $this->serializer->serializeMessage($message);

            $payload['headers'][Header::EVENT_ID] ??= Uuid::uuid4()->toString();

            $messageRow = [
                $this->eventIdEncoder->encodeId($payload['headers'][Header::EVENT_ID]),
                $this->aggregateRootIdEncoder->encodeId($message->aggregateRootId()),
                $payload['headers'][Header::AGGREGATE_ROOT_VERSION] ?? 0,
                json_encode($payload, $this->jsonEncodeOptions),
            ];

            foreach ($additionalColumns as $column => $header) {
                $messageRow[] = $payload['headers'][$header] ?? null;
            }

            $messageRowset[] = $messageRow;
        }

        try {
            $this->table
                ->insertMultiple($insertColumns, $messageRowset);
        } catch (Throwable $exception) {
            throw UnableToPersistMessages::dueTo('', $exception);
        }
    }

    public function retrieveAll(
        AggregateRootId $id,
    ) : Generator {
        try {
            $result = $this->table->select()
                ->columns($this->tableSchema->payloadColumn())
                ->where($this->tableSchema->aggregateRootIdColumn(), $this->aggregateRootIdEncoder->encodeId($id))
                ->orderBy($this->tableSchema->versionColumn(), 'ASC')
                ->run();

            return $this->yieldMessagesFromPayloads($result);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMessages::dueTo('', $exception);
        }
    }

    /** @psalm-return Generator<Message> */
    public function retrieveAllAfterVersion(
        AggregateRootId $id,
        int $aggregateRootVersion,
    ) : Generator {
        try {
            $result = $this->table->select()
                ->columns($this->tableSchema->payloadColumn())
                ->where($this->tableSchema->aggregateRootIdColumn(), $this->aggregateRootIdEncoder->encodeId($id))
                ->andWhere($this->tableSchema->versionColumn(), $aggregateRootVersion)
                ->orderBy($this->tableSchema->versionColumn(), 'ASC')
                ->run();

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

        try {
            $numberOfMessages = 0;

            $incrementalIdColumn = $this->tableSchema->incrementalIdColumn();

            $result = $this->table->select()
                ->columns($this->tableSchema->payloadColumn())
                ->where($incrementalIdColumn, '>', $cursor->offset())
                ->limit($cursor->limit())
                ->orderBy($incrementalIdColumn, 'ASC')
                ->run();

            foreach ($result as $payload) {
                $numberOfMessages++;

                yield $this->serializer->unserializePayload(json_decode($payload['payload'], true));
            }
        } catch (Throwable $exception) {
            throw UnableToRetrieveMessages::dueTo($exception->getMessage(), $exception);
        }

        return $cursor->plusOffset($numberOfMessages);
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
