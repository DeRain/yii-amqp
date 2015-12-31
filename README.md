# yii-amqp
php-amqp wrapper for yii framework

This is [php-amqplib](https://github.com/videlalvaro/php-amqplib) wrapper for yii framework.
Inspired by [musgravehill/yii-amqp](https://github.com/musgravehill/yii-amqp).

*Yii config example:*  
```bash
  'components' => [
            'amqp' => [
                'class' => 'DeRain\yii\Amqp\Amqp',
                'host' => 'localhost',
                'login' => 'guest',
                'password' => 'guest',
                'port' => 5672,
                'vhost' => '/',
            ],
        ],
        ...
```   
