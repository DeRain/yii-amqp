<?php

namespace DeRain\yii\Amqp;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * This is php-amqplib wrapper
 * @link https://github.com/videlalvaro/php-amqplib
 *
 * Configuration example:
 * 'amqp' => [
 *      'class' => 'application.components.Amqp.Amqp',
 *      'host' => 'localhost',
 *      'login' => 'guest',
 *      'password' => 'guest',
 *      'port' => 5672,
 *      'vhost' => '/',
 * ],
 *
 * @author Kirill Beresnev <derainberk@gmail.com>
 */
class Amqp extends \CApplicationComponent
{
    const MESSAGE_PERSISTENT = 2;

    /** @var null|string */
    public $host = null;
    /** @var null|string */
    public $port = null;
    /** @var null|string */
    public $vhost = null;
    /** @var null|string */
    public $login = null;
    /** @var null|string */
    public $password = null;

    /** @var null|AMQPStreamConnection */
    private $_connect = null;
    /** @var null|AMQPChannel */
    private $_channel = null;

    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        $this->checkCredentials();
        $this->initConnection();
    }

    /**
     * Declares exchange
     *
     * @param string $exchange
     * @param string $type
     * @param bool $passive
     * @param bool $durable     The exchange will survive server restarts.
     * @param bool $autoDelete  The exchange won't be deleted once the channel is closed.
     *
     * @return mixed|null
     */
    public function declareExchange(
        $exchange,
        $type = 'direct',
        $passive = false,
        $durable = true,
        $autoDelete = false
    ) {
        return $this->_channel->exchange_declare(
            $exchange,
            $type,
            $passive,
            $durable,
            $autoDelete
        );
    }

    /**
     * Declares queue, creates if needed
     *
     * @param string $name
     * @param bool $passive
     * @param bool $durable
     * @param bool $exclusive
     * @param bool $autoDelete
     *
     * @return mixed|null
     */
    public function declareQueue($name, $passive = false, $durable = true, $exclusive = false, $autoDelete = false)
    {
        return $this->_channel->queue_declare($name, $passive, $durable, $exclusive, $autoDelete);
    }

    /**
     * Binds queue to an exchange
     *
     * @param string $queueName
     * @param string $exchangeName
     * @param string $routingKey
     *
     * @return mixed|null
     */
    public function bindQueueExchanger($queueName, $exchangeName, $routingKey = '')
    {
        $this->_channel->queue_bind($queueName, $exchangeName, $routingKey);
    }

    /**
     * @param string $queueName
     * @param string $exchangeName
     * @param string $routingKey
     */
    public function bindQueue($queueName, $exchangeName, $routingKey = '')
    {
        $this->declareExchange($exchangeName);
        $this->declareQueue($queueName);
        $this->bindQueueExchanger($queueName, $exchangeName, $routingKey);
    }

    /**
     * Publishes a message
     *
     * @param AMQPMessage $message
     * @param string $exchangeName
     * @param string $routingKey
     */
    public function publishAmqpMessage(AMQPMessage $message, $exchangeName, $routingKey = '')
    {
        $this->_channel->basic_publish($message, $exchangeName, $routingKey);
    }

    /**
     * Creates message in AMQP format
     *
     * @param $message
     * @param array $properties
     * @return AMQPMessage
     */
    public function createAmqpMessage($message, array $properties)
    {
        return new AMQPMessage($message, $properties);
    }

    /**
     * @param array $message
     * @param string $exchangeName
     * @param string $routingKey
     */
    public function publishJsonMessage(array $message, $exchangeName, $routingKey = '')
    {

        $encodedMessage = CJSON::encode($message);
        $properties = [
            'content_type'  => 'application/json',
            'delivery_mode' => self::MESSAGE_PERSISTENT,
        ];
        $aqmpMessage = $this->createAmqpMessage($encodedMessage, $properties);
        $this->publishAmqpMessage($aqmpMessage, $exchangeName, $routingKey);
    }

    /**
     * @param string $message
     * @param string $exchangeName
     * @param string $routingKey
     */
    public function publishTextMessage($message, $exchangeName, $routingKey = '')
    {
        $properties = [
            'content_type'  => 'text/plain',
            'delivery_mode' => self::MESSAGE_PERSISTENT,
        ];
        $aqmpMessage = $this->createAmqpMessage($message, $properties);
        $this->publishAmqpMessage($aqmpMessage, $exchangeName, $routingKey);
    }

    /**
     * Closes channel and connection
     */
    public function closeConnection()
    {
        $this->_channel->close();
        $this->_connect->close();
    }

    /**
     * @param $name
     */
    public function exchangeDelete($name)
    {
        $this->_channel->exchange_delete($name);
    }

    /**
     * Starts a queue consumer
     *
     * @param string $queueName     Queue from where to get the messages.
     * @param string $consumerTag   Consumer identifier
     * @param callback|null $callback
     * @param bool $noLocal         Don't receive messages published by this consumer.
     * @param bool $noAck           Tells the server if the consumer will acknowledge the messages.
     * @param bool $exclusive       Request exclusive consumer access, meaning only this consumer can access the queue
     * @param bool $noWait
     *
     * @return mixed|string
     */
    public function consume(
        $queueName,
        $consumerTag,
        $callback,
        $noLocal = false,
        $noAck = false,
        $exclusive = false,
        $noWait = false
    ) {
        $this->_channel->basic_consume($queueName, $consumerTag, $noLocal, $noAck, $exclusive, $noWait, $callback);
    }

    /**
     * Wait for some expected AMQP methods and dispatch to them.
     * Unexpected methods are queued up for later calls to this PHP method.
     *
    *  @param array $allowedMethods
     * @param bool $nonBlocking
     * @param int $timeout
     * @throws \PhpAmqpLib\Exception\AMQPOutOfBoundsException
     * @throws \PhpAmqpLib\Exception\AMQPRuntimeException
     *
     * @return mixed
     */
    public function wait($allowedMethods = null, $nonBlocking = false, $timeout = 0)
    {
        while (count($this->_channel->callbacks)) {
            $this->_channel->wait($allowedMethods, $nonBlocking, $timeout);
        }
    }

    /**
     * @param AMQPMessage $message
     */
    public function markMessageAsDelivered(AMQPMessage $message)
    {
        $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
    }

    /**
     * @throws CException
     */
    private function checkCredentials()
    {
        $errors = [];
        if ($this->login === null) {
            $errors[] = 'AMQP login is not set.';
        }
        if ($this->password === null) {
            $errors[] = 'AMQP password is not set.';
        }
        if ($this->host === null) {
            $errors[] = 'AMQP host is not set.';
        }
        if ($this->vhost === null) {
            $errors[] = 'AMQP vhost is not set.';
        }
        if ($this->host === null) {
            $errors[] = 'AMQP host is not set.';
        }

        if (!empty($errors)) {
            throw new CException(implode(' ', $errors));
        }
    }

    /**
     * Initializes AMQP connection
     */
    private function initConnection()
    {
        $this->_connect = new AMQPStreamConnection(
            $this->host,
            $this->port,
            $this->login,
            $this->password,
            $this->vhost
        );
        $this->_channel = $this->_connect->channel();
    }
}
