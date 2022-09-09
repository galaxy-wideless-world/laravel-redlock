<?php

namespace App\Library;

use Illuminate\Redis\Connections\PhpRedisConnection;

/**
 * @ref https://github.com/ronnylt/redlock-php
 */
class RedLock
{
    private $retryDelay;
    private $retryCount;
    private $clockDriftFactor = 0.01;

    private $quorum;

    private $servers = array();
    /**
     * @var $instances PhpRedisConnection[]
     */
    private $instances = array();
    private $autoRelease = false;

    private $lockRes = [];

    function __construct($autoRelease = false, $retryCount = 1, $retryDelay = 200, array $servers = ['demo'])
    {
//        $this->servers = $servers;

        $this->retryDelay = $retryDelay;
        $this->retryCount = $retryCount;
        $this->autoRelease = $autoRelease;

        $this->quorum  = min(count($servers), (count($servers) / 2 + 1));
    }
    public function __destruct()
    {
        if ($this->autoRelease && $this->lockRes){
            @$this->unlock();
        }
    }

    public function lock($resource, $ttlSeconds)
    {
        $ttl = $ttlSeconds * 1000;
        $this->initInstances();

        $token = uniqid();
        $retry = $this->retryCount;

        do {
            $n = 0;

            $startTime = microtime(true) * 1000;

            foreach ($this->instances as $instance) {
                if ($this->lockInstance($instance, $resource, $token, $ttl)) {
                    $n++;
                }
            }

            # Add 2 milliseconds to the drift to account for Redis expires
            # precision, which is 1 millisecond, plus 1 millisecond min drift
            # for small TTLs.
            $drift = ($ttl * $this->clockDriftFactor) + 2;

            $validityTime = $ttl - (microtime(true) * 1000 - $startTime) - $drift;

            if ($n >= $this->quorum && $validityTime > 0) {
                return $this->lockRes = [
                    'validity' => $validityTime,
                    'resource' => $resource,
                    'token'    => $token,
                ];

            } else {
                foreach ($this->instances as $instance) {
                    $this->unlockInstance($instance, $resource, $token);
                }
            }

            // Wait a random delay before to retry
            //while this is last circle,then do not sleep
            if ($retry>1){
                $delay = mt_rand(floor($this->retryDelay / 2), $this->retryDelay);
                usleep($delay * 1000);
            }

            $retry--;

        } while ($retry > 0);

        return false;
    }

    public function unlock()
    {
        $lock = $this->lockRes;
        $this->initInstances();
        $resource = $lock['resource'];
        $token    = $lock['token'];

        foreach ($this->instances as $instance) {
            $this->unlockInstance($instance, $resource, $token);
        }
    }

    private function initInstances()
    {
        if (empty($this->instances)) {
            return $this->instances[] = app('redis.connection');//single matchine
            foreach ($this->servers as $server) {
                list($host, $port, $timeout) = $server;
                $redis = new \Redis();
                $redis->connect($host, $port, $timeout);

                $this->instances[] = $redis;
            }
        }
    }

    private function lockInstance($instance, $resource, $token, $ttl)
    {
        /**
         * @var $instance PhpRedisConnection
         */
        return $instance->set($resource, $token, 'PX', $ttl, 'NX');
    }

    private function unlockInstance($instance, $resource, $token)
    {
        $script = '
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
        ';


        /**
         * @var $instance PhpRedisConnection
         */
        return $instance->eval($script, 1, $resource, $token);
    }
}
